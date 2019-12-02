/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Ints;
import com.oath.halodb.histo.EstimatedHistogram;

class SegmentNonMemoryPool<E extends HashEntry> extends Segment<E> {

    private static final Logger logger = LoggerFactory.getLogger(SegmentNonMemoryPool.class);

    // maximum hash table size
    private static final int MAX_TABLE_POWER = 30;
    private static final int MAX_TABLE_SIZE = 1 << MAX_TABLE_POWER;

    long size;
    Table table;

    private long hitCount;
    private long missCount;
    private long putAddCount;
    private long putReplaceCount;
    private long removeCount;

    private long threshold;
    private final float loadFactor;

    private long rehashes;
    long evictedEntries;

    private final HashAlgorithm hashAlgorithm;

    private static final boolean throwOOME = true;

    SegmentNonMemoryPool(OffHeapHashTableBuilder<E> builder) {
        super(builder.getEntrySerializer());

        this.hashAlgorithm = builder.getHashAlgorighm();

        int hts = builder.getHashTableSize();
        if (hts <= 0) {
            hts = 8192;
        }
        if (hts < 256) {
            hts = 256;
        }
        int msz = HashTableUtil.roundUpToPowerOf2(hts, MAX_TABLE_POWER);
        table = Table.create(msz, throwOOME);
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
    void release() {
        boolean wasFirst = lock();
        try {
            table.release();
            table = null;
        } finally {
            unlock(wasFirst);
        }
    }

    @Override
    long size() {
        return size;
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
        evictedEntries = 0L;
        hitCount = 0L;
        missCount = 0L;
        putAddCount = 0L;
        putReplaceCount = 0L;
        removeCount = 0L;
    }

    @Override
    long rehashes() {
        return rehashes;
    }

    @Override
    E getEntry(KeyBuffer key) {
        boolean wasFirst = lock();
        try {
            for (long hashEntryAdr = table.getFirst(key.hash());
                 hashEntryAdr != 0L;
                 hashEntryAdr = NonMemoryPoolHashEntries.getNext(hashEntryAdr)) {

                if (sameKey(key.buffer, hashEntryAdr, serializer)) {
                    hitCount++;
                    long sizeAddress = hashEntryAdr + NonMemoryPoolHashEntries.ENTRY_OFF_DATA;
                    long locationAddress = sizeAddress + serializer.sizesSize();
                    return serializer.deserialize(sizeAddress, locationAddress);
                }
            }

            missCount++;
            return null;
        } finally {
            unlock(wasFirst);
        }
    }

    @Override
    boolean containsEntry(KeyBuffer key) {
        boolean wasFirst = lock();
        try {
            for (long hashEntryAdr = table.getFirst(key.hash());
                 hashEntryAdr != 0L;
                 hashEntryAdr = NonMemoryPoolHashEntries.getNext(hashEntryAdr)) {
                if (sameKey(key.buffer, hashEntryAdr, serializer)) {
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
    boolean putEntry(KeyBuffer key, E entry, boolean ifAbsent, E oldEntry) {
        byte[] keybytes = key.buffer;
        long hash = key.hash();
        short keySize = Utils.validateKeySize(keybytes.length);
        long hashEntryAdr = Uns.allocate(HashTableUtil.allocLen(keybytes.length, serializer.entrySize()), throwOOME);
        if (hashEntryAdr == 0L) {
            // entry too large to be inserted or OS is not able to provide enough memory
            removeEntry(key);
            return false;
        }
        // initialize hash entry
        NonMemoryPoolHashEntries.init(hashEntryAdr);
        serializeForPut(keybytes, entry, hashEntryAdr);

        if (putEntry(hashEntryAdr, hash, keySize, ifAbsent, oldEntry)) {
            return true;
        } else {
            // free if we did not insert the entry
            Uns.free(hashEntryAdr);
            return false;
        }
    }

    private boolean putEntry(long newHashEntryAdr, long hash, short keyLen, boolean putIfAbsent, E oldEntry) {
        long removeHashEntryAdr = 0L;
        boolean wasFirst = lock();
        try {
            long hashEntryAdr;
            long prevEntryAdr = 0L;
            for (hashEntryAdr = table.getFirst(hash);
                 hashEntryAdr != 0L;
                 prevEntryAdr = hashEntryAdr, hashEntryAdr = NonMemoryPoolHashEntries.getNext(hashEntryAdr)) {
                if (notSameKey(newHashEntryAdr, keyLen, hashEntryAdr)) {
                    continue;
                }

                // putIfAbsent is true, but key is already present, return.
                if (putIfAbsent) {
                    return false;
                }

                // key already exists, we just need to replace the entry.
                if (oldEntry != null) {
                    // if oldEntry does not match on replace(), don't insert
                    long sizeAddress = hashEntryAdr + NonMemoryPoolHashEntries.ENTRY_OFF_DATA;
                    long locationAddress = sizeAddress + serializer.sizesSize();
                    if (!oldEntry.compare(sizeAddress, locationAddress)) {
                        return false;
                    }
                }

                removeInternal(hashEntryAdr, prevEntryAdr, hash);
                removeHashEntryAdr = hashEntryAdr;

                break;
            }

            // key is not present in the map, therefore we need to add a new entry.
            if (hashEntryAdr == 0L) {

                // key is not present but old entry is not null.
                // we consider this as a mismatch and return.
                if (oldEntry != null) {
                    return false;
                }

                if (size >= threshold) {
                    rehash();
                }

                size++;
            }

            add(newHashEntryAdr, hash);

            if (hashEntryAdr == 0L) {
                putAddCount++;
            } else {
                putReplaceCount++;
            }

            return true;
        } finally {
            unlock(wasFirst);
            if (removeHashEntryAdr != 0L) {
                Uns.free(removeHashEntryAdr);
            }
        }
    }

    private boolean notSameKey(long newHashEntryAdr, short newKeyLen, long hashEntryAdr) {
        short serKeyLen = keyLen(hashEntryAdr, serializer);
        return serKeyLen != newKeyLen
               || !Uns.memoryCompare(hashEntryAdr, NonMemoryPoolHashEntries.ENTRY_OFF_DATA + serializer.entrySize(),
                                     newHashEntryAdr, NonMemoryPoolHashEntries.ENTRY_OFF_DATA + serializer.entrySize(),
                                     serKeyLen);
    }

    private void serializeForPut(byte[] key, E entry, long hashEntryAdr) {
        try {
            // write index meta first, then key
            long sizeAddress = hashEntryAdr + NonMemoryPoolHashEntries.ENTRY_OFF_DATA;
            long locationAddress = sizeAddress + serializer.sizesSize();
            entry.serializeSizes(sizeAddress);
            entry.serializeLocation(locationAddress);
            Uns.copyMemory(key, 0, hashEntryAdr, NonMemoryPoolHashEntries.ENTRY_OFF_DATA + serializer.entrySize(), key.length);
        } catch (Throwable e) {
            freeAndThrow(e, hashEntryAdr);
        }
    }

    private void freeAndThrow(Throwable e, long hashEntryAdr) {
        Uns.free(hashEntryAdr);
        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        }
        if (e instanceof Error) {
            throw (Error) e;
        }
        throw new RuntimeException(e);
    }

    @Override
    void clear() {
        boolean wasFirst = lock();
        try {
            size = 0L;

            long next;
            for (int p = 0; p < table.size(); p++) {
                for (long hashEntryAdr = table.getFirst(p);
                     hashEntryAdr != 0L;
                     hashEntryAdr = next) {
                    next = NonMemoryPoolHashEntries.getNext(hashEntryAdr);
                    Uns.free(hashEntryAdr);
                }
            }

            table.clear();
        } finally {
            unlock(wasFirst);
        }
    }

    @Override
    boolean removeEntry(KeyBuffer key) {
        long removeHashEntryAdr = 0L;
        boolean wasFirst = lock();
        try {
            long prevEntryAdr = 0L;
            for (long hashEntryAdr = table.getFirst(key.hash());
                 hashEntryAdr != 0L;
                 prevEntryAdr = hashEntryAdr, hashEntryAdr = NonMemoryPoolHashEntries.getNext(hashEntryAdr)) {
                if (!sameKey(key.buffer, hashEntryAdr, serializer)) {
                    continue;
                }

                // remove existing entry

                removeHashEntryAdr = hashEntryAdr;
                removeInternal(hashEntryAdr, prevEntryAdr, key.hash());

                size--;
                removeCount++;

                return true;
            }

            return false;
        } finally {
            unlock(wasFirst);
            if (removeHashEntryAdr != 0L) {
                Uns.free(removeHashEntryAdr);
            }
        }
    }

    private void rehash() {
        long start = System.currentTimeMillis();
        Table tab = table;
        int tableSize = tab.size();
        if (tableSize > MAX_TABLE_SIZE) {
            // already at max hash table size
            return;
        }

        Table newTable = Table.create(tableSize * 2, throwOOME);
        if (newTable == null) {
            return;
        }
        long next;

        Hasher hasher = Hasher.create(hashAlgorithm);

        for (int part = 0; part < tableSize; part++) {
            for (long hashEntryAdr = tab.getFirst(part);
                 hashEntryAdr != 0L;
                 hashEntryAdr = next) {

                next = NonMemoryPoolHashEntries.getNext(hashEntryAdr);
                NonMemoryPoolHashEntries.setNext(hashEntryAdr, 0L);
                short keySize = keyLen(hashEntryAdr, serializer);
                long hash = hasher.hash(hashEntryAdr, NonMemoryPoolHashEntries.ENTRY_OFF_DATA + serializer.entrySize(), keySize);
                newTable.addAsHead(hash, hashEntryAdr);
            }
        }

        threshold = (long) (newTable.size() * loadFactor);
        table.release();
        table = newTable;
        rehashes++;
        logger.info("Completed rehashing segment in {} ms.", (System.currentTimeMillis() - start));
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
            table.updateBucketHistogram(hist);
        } finally {
            unlock(wasFirst);
        }
    }

    void getEntryAddresses(int mapSegmentIndex, int nSegments, LongArrayList hashEntryAdrs) {
        boolean wasFirst = lock();
        try {
            for (; nSegments-- > 0 && mapSegmentIndex < table.size(); mapSegmentIndex++) {
                for (long hashEntryAdr = table.getFirst(mapSegmentIndex);
                     hashEntryAdr != 0L;
                     hashEntryAdr = NonMemoryPoolHashEntries.getNext(hashEntryAdr)) {
                    hashEntryAdrs.add(hashEntryAdr);
                }
            }
        } finally {
            unlock(wasFirst);
        }
    }

    static final class Table {

        final int mask;
        final long address;
        private boolean released;

        static Table create(int hashTableSize, boolean throwOOME) {
            int msz = Ints.checkedCast(HashTableUtil.NON_MEMORY_POOL_BUCKET_ENTRY_LEN * hashTableSize);
            long address = Uns.allocate(msz, throwOOME);
            return address != 0L ? new Table(address, hashTableSize) : null;
        }

        private Table(long address, int hashTableSize) {
            this.address = address;
            this.mask = hashTableSize - 1;
            clear();
        }

        void clear() {
            // It's important to initialize the hash table memory.
            // (uninitialized memory will cause problems - endless loops, JVM crashes, damaged data, etc)
            Uns.setMemory(address, 0L, HashTableUtil.NON_MEMORY_POOL_BUCKET_ENTRY_LEN * size(), (byte) 0);
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

        long getFirst(long hash) {
            return Uns.getLong(address, bucketOffset(hash));
        }

        void setFirst(long hash, long hashEntryAdr) {
            Uns.putLong(address, bucketOffset(hash), hashEntryAdr);
        }

        long bucketOffset(long hash) {
            return bucketIndexForHash(hash) * HashTableUtil.NON_MEMORY_POOL_BUCKET_ENTRY_LEN;
        }

        private int bucketIndexForHash(long hash) {
            return (int) (hash & mask);
        }

        void removeLink(long hash, long hashEntryAdr, long prevEntryAdr) {
            long next = NonMemoryPoolHashEntries.getNext(hashEntryAdr);

            removeLinkInternal(hash, hashEntryAdr, prevEntryAdr, next);
        }

        void replaceSentinelLink(long hash, long hashEntryAdr, long prevEntryAdr, long newHashEntryAdr) {
            NonMemoryPoolHashEntries.setNext(newHashEntryAdr, NonMemoryPoolHashEntries.getNext(hashEntryAdr));

            removeLinkInternal(hash, hashEntryAdr, prevEntryAdr, newHashEntryAdr);
        }

        private void removeLinkInternal(long hash, long hashEntryAdr, long prevEntryAdr, long next) {
            long head = getFirst(hash);
            if (head == hashEntryAdr) {
                setFirst(hash, next);
            } else if (prevEntryAdr != 0L) {
                if (prevEntryAdr == -1L) {
                    for (long adr = head;
                         adr != 0L;
                         prevEntryAdr = adr, adr = NonMemoryPoolHashEntries.getNext(adr)) {
                        if (adr == hashEntryAdr) {
                            break;
                        }
                    }
                }
                NonMemoryPoolHashEntries.setNext(prevEntryAdr, next);
            }
        }

        void addAsHead(long hash, long hashEntryAdr) {
            long head = getFirst(hash);
            NonMemoryPoolHashEntries.setNext(hashEntryAdr, head);
            setFirst(hash, hashEntryAdr);
        }

        int size() {
            return mask + 1;
        }

        void updateBucketHistogram(EstimatedHistogram h) {
            for (int i = 0; i < size(); i++) {
                int len = 0;
                for (long adr = getFirst(i); adr != 0L; adr = NonMemoryPoolHashEntries.getNext(adr)) {
                    len++;
                }
                h.add(len + 1);
            }
        }
    }

    private void removeInternal(long hashEntryAdr, long prevEntryAdr, long hash) {
        table.removeLink(hash, hashEntryAdr, prevEntryAdr);
    }

    private void add(long hashEntryAdr, long hash) {
        table.addAsHead(hash, hashEntryAdr);
    }

    @Override
    public String toString() {
        return String.valueOf(size);
    }

    static private short keyLen(long hashEntryAdr, HashEntrySerializer<?> serializer) {
        return serializer.readKeySize(hashEntryAdr + NonMemoryPoolHashEntries.ENTRY_OFF_DATA);
    }

    static boolean sameKey(byte[] key, long hashEntryAdr, HashEntrySerializer<?> serializer) {
        return keyLen(hashEntryAdr, serializer) == key.length && compareKey(key, hashEntryAdr, serializer);
    }



    static private boolean compareKey(byte[] key, long hashEntryAdr, HashEntrySerializer<?> serializer) {
        int blkOff = (int) NonMemoryPoolHashEntries.ENTRY_OFF_DATA + serializer.entrySize();
        int p = 0;
        int endIdx = key.length;
        for (; endIdx - p >= 8; p += 8) {
            if (Uns.getLong(hashEntryAdr, blkOff + p) != Uns.getLongFromByteArray(key, p)) {
                return false;
            }
        }
        for (; endIdx - p >= 4; p += 4) {
            if (Uns.getInt(hashEntryAdr, blkOff + p) != Uns.getIntFromByteArray(key, p)) {
                return false;
            }
        }
        for (; endIdx - p >= 2; p += 2) {
            if (Uns.getShort(hashEntryAdr, blkOff + p) != Uns.getShortFromByteArray(key, p)) {
                return false;
            }
        }
        for (; endIdx - p >= 1; p += 1) {
            if (Uns.getByte(hashEntryAdr, blkOff + p) != key[p]) {
                return false;
            }
        }
        return true;
    }
}
