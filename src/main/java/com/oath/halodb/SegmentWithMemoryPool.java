/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import com.oath.halodb.histo.EstimatedHistogram;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Arjun Mannaly
 */
class SegmentWithMemoryPool<V> extends Segment<V> {

    private static final Logger logger = LoggerFactory.getLogger(SegmentWithMemoryPool.class);

    // maximum hash table size
    private static final int MAX_TABLE_SIZE = 1 << 30;

    private long hitCount = 0;
    private long size = 0;
    private long missCount = 0;
    private long putAddCount = 0;
    private long putReplaceCount = 0;
    private long removeCount = 0;
    private long threshold = 0;
    private final float loadFactor;
    private long rehashes = 0;

    private final List<MemoryPoolChunk> chunks;
    private byte currentChunkIndex = -1;

    private final int chunkSize;

    private final MemoryPoolAddress emptyAddress = new MemoryPoolAddress((byte) -1, -1);

    private MemoryPoolAddress freeListHead = emptyAddress;
    private long freeListSize = 0;

    private final int fixedSlotSize;

    private final HashTableValueSerializer<V> valueSerializer;

    private Table table;

    private final ByteBuffer oldValueBuffer = ByteBuffer.allocate(fixedValueLength);
    private final ByteBuffer newValueBuffer = ByteBuffer.allocate(fixedValueLength);

    private final HashAlgorithm hashAlgorithm;

    SegmentWithMemoryPool(OffHeapHashTableBuilder<V> builder) {
        super(builder.getValueSerializer(), builder.getFixedValueSize(), builder.getFixedKeySize(),
              builder.getHasher());

        this.chunks = new ArrayList<>();
        this.chunkSize = builder.getMemoryPoolChunkSize();
        this.valueSerializer = builder.getValueSerializer();
        this.fixedSlotSize = MemoryPoolHashEntries.HEADER_SIZE + fixedKeyLength + fixedValueLength;
        this.hashAlgorithm = builder.getHashAlgorighm();

        int hts = builder.getHashTableSize();
        if (hts <= 0) {
            hts = 8192;
        }
        if (hts < 256) {
            hts = 256;
        }
        int msz = Ints.checkedCast(HashTableUtil.roundUpToPowerOf2(hts, MAX_TABLE_SIZE));
        table = Table.create(msz);
        if (table == null) {
            throw new RuntimeException("unable to allocate off-heap memory for segment");
        }

        float lf = builder.getLoadFactor();
        if (lf <= .0d) {
            lf = .75f;
        }
        this.loadFactor = lf;
        threshold = (long) ((double) table.size() * loadFactor);
    }

    @Override
    public V getEntry(KeyBuffer key) {
        boolean wasFirst = lock();
        try {
            for (MemoryPoolAddress address = table.getFirst(key.hash());
                 address.chunkIndex >= 0;
                 address = getNext(address)) {

                MemoryPoolChunk chunk = chunks.get(address.chunkIndex);
                if (chunk.compareKey(address.chunkOffset, key.buffer)) {
                    hitCount++;
                    return valueSerializer.deserialize(chunk.readOnlyValueByteBuffer(address.chunkOffset));
                }
            }

            missCount++;
            return null;
        } finally {
            unlock(wasFirst);
        }
    }

    @Override
    public boolean containsEntry(KeyBuffer key) {
        boolean wasFirst = lock();
        try {
            for (MemoryPoolAddress address = table.getFirst(key.hash());
                 address.chunkIndex >= 0;
                 address = getNext(address)) {

                MemoryPoolChunk chunk = chunks.get(address.chunkIndex);
                if (chunk.compareKey(address.chunkOffset, key.buffer)) {
                    hitCount++;
                    return true;
                }
            }

            missCount++;
            return false;
        } finally {
            unlock(wasFirst);
        }
    }

    @Override
    boolean putEntry(byte[] key, V value, long hash, boolean putIfAbsent, V oldValue) {
        boolean wasFirst = lock();
        try {
            if (oldValue != null) {
                oldValueBuffer.clear();
                valueSerializer.serialize(oldValue, oldValueBuffer);
            }
            newValueBuffer.clear();
            valueSerializer.serialize(value, newValueBuffer);

            MemoryPoolAddress first = table.getFirst(hash);
            for (MemoryPoolAddress address = first; address.chunkIndex >= 0; address = getNext(address)) {
                MemoryPoolChunk chunk = chunks.get(address.chunkIndex);
                if (chunk.compareKey(address.chunkOffset, key)) {
                    // key is already present in the segment. 

                    // putIfAbsent is true, but key is already present, return.
                    if (putIfAbsent) {
                        return false;
                    }

                    // code for replace() operation
                    if (oldValue != null) {
                        if (!chunk.compareValue(address.chunkOffset, oldValueBuffer.array())) {
                            return false;
                        }
                    }

                    // replace value with the new one.
                    chunk.setValue(newValueBuffer.array(), address.chunkOffset);
                    putReplaceCount++;
                    return true;
                }
            }

            if (oldValue != null) {
                // key is not present but old value is not null.
                // we consider this as a mismatch and return.
                return false;
            }

            if (size >= threshold) {
                rehash();
                first = table.getFirst(hash);
            }

            // key is not present in the segment, we need to add a new entry.
            MemoryPoolAddress nextSlot = writeToFreeSlot(key, newValueBuffer.array(), first);
            table.addAsHead(hash, nextSlot);
            size++;
            putAddCount++;
        } finally {
            unlock(wasFirst);
        }

        return true;
    }

    @Override
    public boolean removeEntry(KeyBuffer key) {
        boolean wasFirst = lock();
        try {
            MemoryPoolAddress previous = null;
            for (MemoryPoolAddress address = table.getFirst(key.hash());
                 address.chunkIndex >= 0;
                 previous = address, address = getNext(address)) {

                MemoryPoolChunk chunk = chunks.get(address.chunkIndex);
                if (chunk.compareKey(address.chunkOffset, key.buffer)) {
                    removeInternal(address, previous, key.hash());
                    removeCount++;
                    size--;
                    return true;
                }
            }

            return false;
        } finally {
            unlock(wasFirst);
        }
    }

    private MemoryPoolAddress getNext(MemoryPoolAddress address) {
        if (address.chunkIndex < 0 || address.chunkIndex >= chunks.size()) {
            throw new IllegalArgumentException("Invalid chunk index " + address.chunkIndex + ". Chunk size " + chunks.size());
        }

        MemoryPoolChunk chunk = chunks.get(address.chunkIndex);
        return chunk.getNextAddress(address.chunkOffset);
    }

    private MemoryPoolAddress writeToFreeSlot(byte[] key, byte[] value, MemoryPoolAddress nextAddress) {
        if (!freeListHead.equals(emptyAddress)) {
            // write to the head of the free list.
            MemoryPoolAddress temp = freeListHead;
            freeListHead = chunks.get(freeListHead.chunkIndex).getNextAddress(freeListHead.chunkOffset);
            chunks.get(temp.chunkIndex).fillSlot(temp.chunkOffset, key, value, nextAddress);
            --freeListSize;
            return temp;
        }

        if (currentChunkIndex == -1 || chunks.get(currentChunkIndex).remaining() < fixedSlotSize) {
            if (chunks.size() > Byte.MAX_VALUE) {
                logger.error("No more memory left. Each segment can have at most {} chunks.", Byte.MAX_VALUE + 1);
                throw new OutOfMemoryError("Each segment can have at most " + (Byte.MAX_VALUE + 1) + " chunks.");
            }

            // There is no chunk allocated for this segment or the current chunk being written to has no space left.
            // allocate an new one. 
            chunks.add(MemoryPoolChunk.create(chunkSize, fixedKeyLength, fixedValueLength));
            ++currentChunkIndex;
        }

        MemoryPoolChunk currentWriteChunk = chunks.get(currentChunkIndex);
        MemoryPoolAddress slotAddress = new MemoryPoolAddress(currentChunkIndex, currentWriteChunk.getWriteOffset());
        currentWriteChunk.fillNextSlot(key, value, nextAddress);
        return slotAddress;
    }

    private void removeInternal(MemoryPoolAddress address, MemoryPoolAddress previous, long hash) {
        MemoryPoolAddress next = chunks.get(address.chunkIndex).getNextAddress(address.chunkOffset);
        if (table.getFirst(hash).equals(address)) {
            table.addAsHead(hash, next);
        } else if (previous == null) {
            //this should never happen. 
            throw new IllegalArgumentException("Removing entry which is not head but with previous null");
        } else {
            chunks.get(previous.chunkIndex).setNextAddress(previous.chunkOffset, next);
        }

        chunks.get(address.chunkIndex).setNextAddress(address.chunkOffset, freeListHead);
        freeListHead = address;
        ++freeListSize;
    }

    private void rehash() {
        long start = System.currentTimeMillis();
        Table currentTable = table;
        int tableSize = currentTable.size();
        if (tableSize > MAX_TABLE_SIZE) {
            return;
        }

        Table newTable = Table.create(tableSize * 2);
        Hasher hasher = Hasher.create(hashAlgorithm);
        MemoryPoolAddress next;

        for (int i = 0; i < tableSize; i++) {
            for (MemoryPoolAddress address = table.getFirst(i); address.chunkIndex >= 0; address = next) {
                long hash = chunks.get(address.chunkIndex).computeHash(address.chunkOffset, hasher);
                next = getNext(address);
                MemoryPoolAddress first = newTable.getFirst(hash);
                newTable.addAsHead(hash, address);
                chunks.get(address.chunkIndex).setNextAddress(address.chunkOffset, first);
            }
        }

        threshold = (long) ((float) newTable.size() * loadFactor);
        table.release();
        table = newTable;
        rehashes++;

        logger.info("Completed rehashing segment in {} ms.", (System.currentTimeMillis() - start));
    }

    @Override
    long size() {
        return size;
    }

    @Override
    void release() {
        boolean wasFirst = lock();
        try {
            chunks.forEach(MemoryPoolChunk::destroy);
            chunks.clear();
            currentChunkIndex = -1;
            size = 0;
            table.release();
        } finally {
            unlock(wasFirst);
        }

    }

    @Override
    void clear() {
        boolean wasFirst = lock();
        try {
            chunks.forEach(MemoryPoolChunk::destroy);
            chunks.clear();
            currentChunkIndex = -1;
            size = 0;
            table.clear();
        } finally {
            unlock(wasFirst);
        }
    }

    @Override
    long hitCount() {
        return hitCount;
    }

    @Override
    long missCount() {
        return missCount;
    }

    @Override
    long putAddCount() {
        return putAddCount;
    }

    @Override
    long putReplaceCount() {
        return putReplaceCount;
    }

    @Override
    long removeCount() {
        return removeCount;
    }

    @Override
    void resetStatistics() {
        rehashes = 0L;
        hitCount = 0L;
        missCount = 0L;
        putAddCount = 0L;
        putReplaceCount = 0L;
        removeCount = 0L;
    }

    @Override
    long numberOfChunks() {
        return chunks.size();
    }

    @Override
    long numberOfSlots() {
        return chunks.size() * chunkSize / fixedSlotSize;
    }

    @Override
    long freeListSize() {
        return freeListSize;
    }

    @Override
    long rehashes() {
        return rehashes;
    }

    @Override
    float loadFactor() {
        return loadFactor;
    }

    @Override
    int hashTableSize() {
        return table.size();
    }

    @Override
    void updateBucketHistogram(EstimatedHistogram hist) {
        boolean wasFirst = lock();
        try {
            table.updateBucketHistogram(hist, chunks);
        } finally {
            unlock(wasFirst);
        }
    }

    static final class Table {

        final int mask;
        final long address;
        private boolean released;

        static Table create(int hashTableSize) {
            int msz = Ints.checkedCast(HashTableUtil.MEMORY_POOL_BUCKET_ENTRY_LEN * hashTableSize);
            long address = Uns.allocate(msz, true);
            return address != 0L ? new Table(address, hashTableSize) : null;
        }

        private Table(long address, int hashTableSize) {
            this.address = address;
            this.mask = hashTableSize - 1;
            clear();
        }

        void clear() {
            Uns.setMemory(address, 0L, HashTableUtil.MEMORY_POOL_BUCKET_ENTRY_LEN * size(), (byte) -1);
        }

        void release() {
            Uns.free(address);
            released = true;
        }

        protected void finalize() throws Throwable {
            if (!released) {
                Uns.free(address);
            }
            super.finalize();
        }

        MemoryPoolAddress getFirst(long hash) {
            long bOffset = address + bucketOffset(hash);
            byte chunkIndex = Uns.getByte(bOffset, 0);
            int chunkOffset = Uns.getInt(bOffset, 1);
            return new MemoryPoolAddress(chunkIndex, chunkOffset);

        }

        void addAsHead(long hash, MemoryPoolAddress entryAddress) {
            long bOffset = address + bucketOffset(hash);
            Uns.putByte(bOffset, 0, entryAddress.chunkIndex);
            Uns.putInt(bOffset, 1, entryAddress.chunkOffset);
        }

        long bucketOffset(long hash) {
            return bucketIndexForHash(hash) * HashTableUtil.MEMORY_POOL_BUCKET_ENTRY_LEN;
        }

        private int bucketIndexForHash(long hash) {
            return (int) (hash & mask);
        }

        int size() {
            return mask + 1;
        }

        void updateBucketHistogram(EstimatedHistogram h, final List<MemoryPoolChunk> chunks) {
            for (int i = 0; i < size(); i++) {
                int len = 0;
                for (MemoryPoolAddress adr = getFirst(i); adr.chunkIndex >= 0;
                     adr = chunks.get(adr.chunkIndex).getNextAddress(adr.chunkOffset)) {
                    len++;
                }
                h.add(len + 1);
            }
        }
    }

    @VisibleForTesting
    MemoryPoolAddress getFreeListHead() {
        return freeListHead;
    }

    @VisibleForTesting
    int getChunkWriteOffset(int index) {
        return chunks.get(index).getWriteOffset();
    }
}
