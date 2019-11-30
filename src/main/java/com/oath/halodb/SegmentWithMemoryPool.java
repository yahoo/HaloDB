/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import com.oath.halodb.histo.EstimatedHistogram;

class SegmentWithMemoryPool<E extends HashEntry> extends Segment<E> {

    private static final Logger logger = LoggerFactory.getLogger(SegmentWithMemoryPool.class);

    // maximum hash table size
    private static final int MAX_TABLE_POWER = 30;
    private static final int MAX_TABLE_SIZE = 1 << MAX_TABLE_POWER;

    private long hitCount = 0;
    private long size = 0;
    private long missCount = 0;
    private long putAddCount = 0;
    private long putReplaceCount = 0;
    private long removeCount = 0;
    private long threshold = 0;
    private final float loadFactor;
    private long rehashes = 0;

    private final List<MemoryPoolChunk<E>> chunks = new ArrayList<>();
    private MemoryPoolChunk<E> currentWriteChunk = null;

    private final int chunkSize;

    private MemoryPoolAddress freeListHead = MemoryPoolAddress.empty;
    private long freeListSize = 0;

    private final int slotSize;

    private final HashEntrySerializer<E> serializer;

    private Table table;

    private final HashAlgorithm hashAlgorithm;

    SegmentWithMemoryPool(OffHeapHashTableBuilder<E> builder) {
        super(builder.getEntrySerializer(), builder.getFixedKeySize(), builder.getHasher());

        this.chunkSize = builder.getMemoryPoolChunkSize();
        this.serializer = builder.getEntrySerializer();
        this.slotSize = MemoryPoolHashEntries.HEADER_SIZE + fixedKeyLength + serializer.entrySize();
        this.hashAlgorithm = builder.getHashAlgorighm();

        int hts = builder.getHashTableSize();
        if (hts <= 0) {
            hts = 8192;
        }
        if (hts < 256) {
            hts = 256;
        }
        table = Table.create(hts);
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
    public E getEntry(KeyBuffer key) {
        boolean wasFirst = lock();
        try {
            for (MemoryPoolAddress address = table.getFirst(key.hash());
                 address.chunkIndex > 0;
                 address = getNext(address)) {

                MemoryPoolChunk<E> chunk = chunkFor(address);
                if (chunk.compareKey(address.chunkOffset, key.buffer)) {
                    hitCount++;
                    return chunk.readEntry(address.chunkOffset);
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
                 address.chunkIndex > 0;
                 address = getNext(address)) {

                MemoryPoolChunk<?> chunk = chunkFor(address);
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
    boolean putEntry(byte[] key, E entry, long hash, boolean putIfAbsent, E oldEntry) {
        boolean wasFirst = lock();
        try {
            MemoryPoolAddress first = table.getFirst(hash);
            for (MemoryPoolAddress address = first; address.chunkIndex > 0; address = getNext(address)) {
                MemoryPoolChunk<E> chunk = chunkFor(address);
                if (chunk.compareKey(address.chunkOffset, key)) {
                    // key is already present in the segment.

                    // putIfAbsent is true, but key is already present, return.
                    if (putIfAbsent) {
                        return false;
                    }

                    // code for replace() operation
                    if (oldEntry != null) {
                        if (!chunk.compareEntry(address.chunkOffset, oldEntry)) {
                            return false;
                        }
                    }

                    // replace value with the new one.
                    chunk.setEntry(address.chunkOffset, entry);
                    putReplaceCount++;
                    return true;
                }
            }

            if (oldEntry != null) {
                // key is not present but old value is not null.
                // we consider this as a mismatch and return.
                return false;
            }

            if (size >= threshold) {
                rehash();
                first = table.getFirst(hash);
            }

            // key is not present in the segment, we need to add a new entry.
            MemoryPoolAddress nextSlot = writeToFreeSlot(key, entry, first);
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
                 address.chunkIndex > 0;
                 previous = address, address = getNext(address)) {

                MemoryPoolChunk<E> chunk = chunkFor(address);
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

    private MemoryPoolChunk<E> chunkFor(MemoryPoolAddress poolAddress) {
        return chunkFor(poolAddress.chunkIndex);
    }

    private MemoryPoolChunk<E> chunkFor(int chunkIndex) {
        if (chunkIndex < 1 || chunkIndex > chunks.size()) {
            throw new IllegalArgumentException("Invalid chunk index " + chunkIndex + ". Chunk size " + chunks.size());
        }
        return chunks.get(chunkIndex - 1);
    }

    private MemoryPoolAddress getNext(MemoryPoolAddress address) {
        MemoryPoolChunk<E> chunk = chunkFor(address);
        return chunk.getNextAddress(address.chunkOffset);
    }

    private MemoryPoolAddress writeToFreeSlot(byte[] key, E entry, MemoryPoolAddress nextAddress) {
        if (!freeListHead.isEmpty()) {
            // write to the head of the free list.
            MemoryPoolAddress temp = freeListHead;
            freeListHead = getNext(freeListHead);
            chunkFor(temp).fillSlot(temp.chunkOffset, key, entry, nextAddress);
            --freeListSize;
            return temp;
        }

        if (currentWriteChunk == null || currentWriteChunk.remaining() < slotSize) {
            if (chunks.size() >= 255) {
                logger.error("No more memory left. Each segment can have at most {} chunks.", 255);
                throw new OutOfMemoryError("Each segment can have at most " + 255 + " chunks.");
            }

            // There is no chunk allocated for this segment or the current chunk being written to has no space left.
            // allocate an new one.
            currentWriteChunk = MemoryPoolChunk.create(chunkSize, fixedKeyLength, serializer);
            chunks.add(currentWriteChunk);
        }

        MemoryPoolAddress slotAddress = new MemoryPoolAddress((byte) chunks.size(), currentWriteChunk.getWriteOffset());
        currentWriteChunk.fillNextSlot(key, entry, nextAddress);
        return slotAddress;
    }

    private void removeInternal(MemoryPoolAddress address, MemoryPoolAddress previous, long hash) {
        MemoryPoolAddress next = getNext(address);
        if (table.getFirst(hash).equals(address)) {
            table.addAsHead(hash, next);
        } else if (previous == null) {
            //this should never happen.
            throw new IllegalArgumentException("Removing entry which is not head but with previous null");
        } else {
            chunkFor(previous).setNextAddress(previous.chunkOffset, next);
        }

        chunkFor(address).setNextAddress(address.chunkOffset, freeListHead);
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
            for (MemoryPoolAddress address = table.getFirst(i); address.chunkIndex > 0; address = next) {
                long hash = chunkFor(address).computeHash(address.chunkOffset, hasher);
                next = getNext(address);
                MemoryPoolAddress first = newTable.getFirst(hash);
                newTable.addAsHead(hash, address);
                chunkFor(address).setNextAddress(address.chunkOffset, first);
            }
        }

        threshold = (long) (newTable.size() * loadFactor);
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
            currentWriteChunk = null;
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
            currentWriteChunk = null;
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
        return chunks.size() * chunkSize / slotSize;
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
            table.updateBucketHistogram(hist, this);
        } finally {
            unlock(wasFirst);
        }
    }

    static final class Table {

        final int mask;
        final long address;
        private boolean released;

        static Table create(int hashTableSize) {
            int pow2Size = HashTableUtil.roundUpToPowerOf2(hashTableSize, MAX_TABLE_POWER);

            int msz = Ints.checkedCast(HashTableUtil.MEMORY_POOL_BUCKET_ENTRY_LEN * pow2Size);
            long address = Uns.allocate(msz, true);
            return address != 0L ? new Table(address, pow2Size) : null;
        }

        private Table(long address, int pow2Size) {
            this.address = address;
            this.mask = pow2Size - 1;
            clear();
        }

        void clear() {
            Uns.setMemory(address, 0L, HashTableUtil.MEMORY_POOL_BUCKET_ENTRY_LEN * size(), (byte) 0);
        }

        void release() {
            Uns.free(address);
            released = true;
        }

        @Override
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
            Uns.putByte(bOffset, 0, (byte) entryAddress.chunkIndex);
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

        <E extends HashEntry> void updateBucketHistogram(EstimatedHistogram h, final SegmentWithMemoryPool<E> segment) {
            for (int i = 0; i < size(); i++) {
                int len = 0;
                for (MemoryPoolAddress adr = getFirst(i); adr.chunkIndex > 0;
                     adr = segment.getNext(adr)) {
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
