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
        super(builder.getEntrySerializer(), builder.getFixedKeySize());

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
        return search(key, this::foundEntry, this::notFoundEntry);
    }

    private E foundEntry(MemoryPoolChunk<E> headChunk, MemoryPoolAddress headAddress, MemoryPoolAddress previous, MemoryPoolAddress tail, int chainLength) {
        hitCount++;
        return headChunk.readEntry(headAddress.chunkOffset);
    }

    private E notFoundEntry(MemoryPoolAddress slot) {
        missCount++;
        return null;
    }

    @Override
    public boolean containsEntry(KeyBuffer key) {
        return search(key, this::foundKey, this::notFoundKey);
    }

    private boolean foundKey(MemoryPoolChunk<E> headChunk, MemoryPoolAddress headAddress, MemoryPoolAddress previous, MemoryPoolAddress tail, int chainLength) {
        hitCount++;
        return true;
    }

    private boolean notFoundKey(MemoryPoolAddress slot) {
        missCount++;
        return false;
    }

    @Override
    boolean putEntry(KeyBuffer key, E entry, boolean dontOverwrite, E oldEntry) {
        return search(key,
            (headChunk, headAddress, previous, tail, chainLength) -> {
                // key is already present in the segment.

                // putIfAbsent is true, but key is already present, return.
                if (dontOverwrite) {
                    return false;
                }

                // code for replace() operation
                if (oldEntry != null) {
                    if (!headChunk.compareEntry(headAddress.chunkOffset, oldEntry)) {
                        return false;
                    }
                }

                // replace value with the new one.
                headChunk.setEntry(headAddress.chunkOffset, entry);
                putReplaceCount++;
                return true;
            },
            (slotHead) ->  {
                // key is not present
                if (oldEntry != null) {
                    // key is not present but old value is not null.
                    // we consider this as a mismatch and return.
                    return false;
                }

                long hash = key.hash();
                if (size >= threshold) {
                    rehash();
                    slotHead = table.getFirst(hash);
                }

                // key is not present in the segment, we need to add a new entry.
                MemoryPoolAddress nextSlot = writeToFreeSlots(key.buffer, entry, slotHead);
                table.addAsHead(hash, nextSlot);
                size++;
                putAddCount++;
                return true;
            });
    }


    @Override
    public boolean removeEntry(KeyBuffer key) {
        return search(key,
            (headChunk, headAddress, previous, tail, chainLength) -> {
                removeInternal(headAddress, previous, tail, chainLength, key.hash());
                removeCount++;
                size--;
                return true;
            }, (first) -> {
                return false;
            });
    }

    @FunctionalInterface
    private interface FoundEntryVisitor<E extends HashEntry, A> {
        /**
         * @param headChunk The chunk corresponding to the headAddress
         * @param headAddress  The address of the first slot containing the key
         * @param previous  The address of the slot in the list prior to the key, if it exists
         * @param tail  The last slot in the chain for this key, the same as the headAddress if the chain is size 1
         * @param chainLength  The number of slots in the chain for the key
         * @return  The result that the search function will return when the key is found.
         */
        A found(MemoryPoolChunk<E> headChunk, MemoryPoolAddress headAddress, MemoryPoolAddress previous, MemoryPoolAddress tail, int chainLength);
    }

    @FunctionalInterface
    private interface NotFoundEntryVisitor<A> {
        /**
         *
         * @param firstAddress  The first address for the slot corresponding to the hash of this key
         * @return  The result that the search function will return when the key is not found.
         */
        A notFound(MemoryPoolAddress firstAddress);
    }

    private <A> A search(KeyBuffer key,
                         FoundEntryVisitor<E, A> whenFound,
                         NotFoundEntryVisitor<A> whenNotFound) {
        boolean wasFirst = lock();
        try {
            MemoryPoolAddress previous = null;
            MemoryPoolAddress firstAddress = table.getFirst(key.hash());
            MemoryPoolAddress address = firstAddress;
            while (!address.isEmpty()) {
                MemoryPoolChunk<E> chunk = chunkFor(address);
                int ksize = key.buffer.length;
                int slotKeySize = chunk.getKeyLength(address.chunkOffset);
                if (slotKeySize <= fixedKeyLength) {
                    // one slot, simple match and move on
                    if (slotKeySize == ksize && chunk.compareFixedKey(address.chunkOffset, key.buffer, ksize)) {
                        return whenFound.found(chunk, address, previous, address, 1);
                    }
                } else {
                    // multiple slots, we must always traverse to the end of the chain for this key, even when it mismatches
                    int chainLength = 1;
                    MemoryPoolChunk<E> headChunk = chunk;
                    MemoryPoolAddress headAddress = address;
                    int remaining = slotKeySize - fixedKeyLength;
                    int maxFragmentSize = fixedKeyLength + serializer.entrySize();
                    boolean fragmentMatches = slotKeySize == ksize && chunk.compareFixedKey(address.chunkOffset, key.buffer, fixedKeyLength);
                    do {
                        address = chunk.getNextAddress(address.chunkOffset);
                        if (address.isEmpty()) {
                            throw new IllegalStateException("Corrupted slot state, extended key slot expected, found none");
                        }
                        if (fragmentMatches) {
                            chunk = chunkFor(address);
                            int compareOffset = ksize - remaining;
                            int compareLen = Math.min(maxFragmentSize, remaining);
                            fragmentMatches = chunk.compareExtendedKey(address.chunkOffset, key.buffer, compareOffset, compareLen);
                            chainLength++;
                        }
                        remaining -= maxFragmentSize;
                    } while (remaining > 0);
                    // we got through the key and all fragments matched, key found
                    if (fragmentMatches) {
                        return whenFound.found(headChunk, headAddress, previous, address, chainLength);
                    }
                }
                previous = address;
                address = chunk.getNextAddress(address.chunkOffset);
            }
            return whenNotFound.notFound(firstAddress);
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

    private MemoryPoolAddress writeToFreeSlots(byte[] key, E entry, MemoryPoolAddress nextAddress) {
        MemoryPoolAddress firstSlot = getFreeSlot();
        MemoryPoolAddress slot = firstSlot;
        MemoryPoolAddress next = (key.length <= fixedKeyLength) ? nextAddress : getFreeSlot();

        chunkFor(slot).fillSlot(slot.chunkOffset, key, entry, next);

        int keyWritten = fixedKeyLength;
        while (keyWritten < key.length) {
            slot = next;
            int keyRemaining = key.length - keyWritten;
            int overflowSlotSpace = fixedKeyLength + serializer.entrySize();
            if (keyRemaining > overflowSlotSpace) {
                next = getFreeSlot();
                chunkFor(slot).fillOverflowSlot(slot.chunkOffset, key, keyWritten, overflowSlotSpace, next);
            } else {
                chunkFor(slot).fillOverflowSlot(slot.chunkOffset, key, keyWritten, keyRemaining, nextAddress);
            }
            keyWritten += overflowSlotSpace;
        }
        return firstSlot;
    }

    MemoryPoolAddress getFreeSlot() {
        if (!freeListHead.isEmpty() ) {
            MemoryPoolAddress free = freeListHead;
            freeListHead = getNext(free);
            freeListSize--;
            return free;
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
        return new MemoryPoolAddress((byte) chunks.size(), currentWriteChunk.allocateSlot());
    }

    private void removeInternal(MemoryPoolAddress head, MemoryPoolAddress previous, MemoryPoolAddress tail, int length, long hash) {
        MemoryPoolAddress next = getNext(tail);
        if (previous == null) {
            table.addAsHead(hash, next);
        } else {
            chunkFor(previous).setNextAddress(previous.chunkOffset, next);
        }

        chunkFor(tail).setNextAddress(tail.chunkOffset, freeListHead);
        freeListHead = head;
        freeListSize += length;
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

        long hashBuffer = Uns.allocate(2048, true); // larger than max key size
        try {
            for (int i = 0; i < tableSize; i++) {
                // each table slot is a chain of entries, individual keys can span more than one entry if the key
                // size is larger than fixedKeyLength
                MemoryPoolAddress address = table.getFirst(i);
                while (!address.isEmpty()) {
                    MemoryPoolChunk<E> chunk = chunkFor(address);
                    MemoryPoolAddress headAddress = address;
                    int keySize = chunk.getKeyLength(address.chunkOffset);
                    long hash;
                    if (keySize <= fixedKeyLength) {
                        // hash calculation is simple if the key fits in one slot
                        hash = chunk.computeFixedKeyHash(address.chunkOffset, hasher, keySize);
                    } else {
                        // otherwise, since hasher doesn't support incremental hashes, we have to copy the data to a buffer
                        // then hash
                        chunk.copyEntireFixedKey(address.chunkOffset, hashBuffer);
                        int copied = fixedKeyLength;
                        do {
                            address = getNext(address);
                            chunk = chunkFor(address);
                            copied += chunk.copyExtendedKey(address.chunkOffset, hashBuffer, copied, keySize - copied);
                        } while (copied < keySize);
                        hash = hasher.hash(hashBuffer, 0, keySize);
                    }
                    // get the address the tail of this key points to
                    MemoryPoolAddress next = getNext(address);
                    MemoryPoolAddress first = newTable.getFirst(hash);
                    // put the head of this key as the entry in the table
                    newTable.addAsHead(hash, headAddress);
                    // set the tail of this key to point to whatever was in the head of the new table
                    chunk.setNextAddress(address.chunkOffset, first);
                    address = next;
                }
            }
        } finally {
            Uns.free(hashBuffer);
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
