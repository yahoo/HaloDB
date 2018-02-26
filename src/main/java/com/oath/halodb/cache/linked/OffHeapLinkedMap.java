/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

/*
 *      Copyright (C) 2014 Robert Stupp, Koeln, Germany, robert-stupp.de
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

/*
 * Modified for HaloDB.
 */
package com.oath.halodb.cache.linked;

import com.google.common.primitives.Ints;
import com.oath.halodb.cache.HashAlgorithm;
import com.oath.halodb.cache.OHCacheBuilder;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import com.oath.halodb.cache.CacheSerializer;
import com.oath.halodb.cache.histo.EstimatedHistogram;

class OffHeapLinkedMap<V> {
    // maximum hash table size
    private static final int MAX_TABLE_SIZE = 1 << 30;

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
    private long expiredEntries;

    private final CacheSerializer<V> valueSerializer;
    private final int fixedValueLength;

    private final HashAlgorithm hashAlgorithm;

    // Replacement for Unsafe.monitorEnter/monitorExit. Uses the thread-ID to indicate a lock
    // using a CAS operation on the primitive instance field.
    private final boolean unlocked;
    private volatile long lock;
    private static final AtomicLongFieldUpdater<OffHeapLinkedMap> lockFieldUpdater =
    AtomicLongFieldUpdater.newUpdater(OffHeapLinkedMap.class, "lock");

    private final boolean throwOOME;

    OffHeapLinkedMap(OHCacheBuilder<?,V> builder)
    {
        this.throwOOME = builder.isThrowOOME();

        this.unlocked = builder.isUnlocked();

        this.valueSerializer = builder.getValueSerializer();
        this.fixedValueLength = builder.getFixedValueSize();

        this.hashAlgorithm = builder.getHashAlgorighm();
        
        int hts = builder.getHashTableSize();
        if (hts <= 0)
            hts = 8192;
        if (hts < 256)
            hts = 256;
        int msz = Ints.checkedCast(Util.roundUpToPowerOf2(hts, MAX_TABLE_SIZE));
        table = Table.create(msz, throwOOME);
        if (table == null)
            throw new RuntimeException("unable to allocate off-heap memory for segment");

        float lf = builder.getLoadFactor();
        if (lf <= .0d)
            lf = .75f;
        this.loadFactor = lf;
        threshold = (long) ((double) table.size() * loadFactor);
    }

    void release()
    {
        boolean wasFirst = lock();
        try
        {
            table.release();
            table = null;
        }
        finally
        {
            unlock(wasFirst); 
        }
    }

    long size()
    {
        return size;
    }

    long hitCount()
    {
        return hitCount;
    }

    long missCount()
    {
        return missCount;
    }

    long putAddCount()
    {
        return putAddCount;
    }

    long putReplaceCount()
    {
        return putReplaceCount;
    }

    long removeCount()
    {
        return removeCount;
    }

    void resetStatistics()
    {
        rehashes = 0L;
        evictedEntries = 0L;
        hitCount = 0L;
        missCount = 0L;
        putAddCount = 0L;
        putReplaceCount = 0L;
        removeCount = 0L;
    }

    long rehashes()
    {
        return rehashes;
    }

    long evictedEntries()
    {
        return evictedEntries;
    }

    long expiredEntries()
    {
        return expiredEntries;
    }

    V getEntry(KeyBuffer key) {
        boolean wasFirst = lock();
        try {
            for (long hashEntryAdr = table.getFirst(key.hash());
                 hashEntryAdr != 0L;
                 hashEntryAdr = HashEntries.getNext(hashEntryAdr)) {

                if (key.sameKey(hashEntryAdr)) {
                    hitCount++;
                    return valueSerializer.deserialize(Uns.valueBufferR(hashEntryAdr, fixedValueLength));
                }
            }

            missCount++;
            return null;
        }
        finally {
            unlock(wasFirst);
        }
    }

    boolean containsEntry(KeyBuffer key) {
        boolean wasFirst = lock();
        try {
            for (long hashEntryAdr = table.getFirst(key.hash());
                 hashEntryAdr != 0L;
                 hashEntryAdr = HashEntries.getNext(hashEntryAdr)) {
                if (key.sameKey(hashEntryAdr)) {
                    hitCount++;
                    return true;
                }
            }

            missCount++;
            return false;
        }
        finally {
            unlock(wasFirst);
        }
    }

    boolean putEntry(long newHashEntryAdr, long hash, long keyLen, long bytes, boolean putIfAbsent,
                     long oldValueAddr, long valueSize)
    {
        long removeHashEntryAdr = 0L;
        boolean wasFirst = lock();
        try
        {
            long hashEntryAdr;
            long prevEntryAdr = 0L;
            for (hashEntryAdr = table.getFirst(hash);
                 hashEntryAdr != 0L;
                 prevEntryAdr = hashEntryAdr, hashEntryAdr = HashEntries.getNext(hashEntryAdr))
            {
                if (notSameKey(newHashEntryAdr, hash, keyLen, hashEntryAdr))
                    continue;

                // putIfAbsent is true, but key is already present, return.
                if(putIfAbsent)
                    return false;

                // key already exists, we just need to replace the value.
                if (oldValueAddr != 0L)
                {
                    // code for replace() operation
                    if (!Uns.memoryCompare(hashEntryAdr, HashEntries.ENTRY_OFF_DATA + keyLen, oldValueAddr, 0L, valueSize))
                        return false;
                }

                removeInternal(hashEntryAdr, prevEntryAdr, hash);
                removeHashEntryAdr = hashEntryAdr;

                break;
            }

            // key is not present in the map, therefore we need to add a new entry.
            if (hashEntryAdr == 0L)
            {

                // key is not present but old value is not null.
                // we consider this as a mismatch and return.
                if (oldValueAddr != 0) {
                    return false;
                }

                if (size >= threshold)
                    rehash();

                size++;
            }

            add(newHashEntryAdr, hash);

            if (hashEntryAdr == 0L)
                putAddCount++;
            else
                putReplaceCount++;

            return true;
        }
        finally
        {
            unlock(wasFirst); 
            if (removeHashEntryAdr != 0L)
                Uns.free(removeHashEntryAdr);
        }
    }

    private static boolean notSameKey(long newHashEntryAdr, long newHash, long newKeyLen, long hashEntryAdr)
    {
        long serKeyLen = HashEntries.getKeyLen(hashEntryAdr);
        return serKeyLen != newKeyLen
               || !Uns.memoryCompare(hashEntryAdr, HashEntries.ENTRY_OFF_DATA, newHashEntryAdr, HashEntries.ENTRY_OFF_DATA, serKeyLen);
    }

    void clear()
    {
        boolean wasFirst = lock();
        try
        {
            size = 0L;

            long next;
            for (int p = 0; p < table.size(); p++)
                for (long hashEntryAdr = table.getFirst(p);
                     hashEntryAdr != 0L;
                     hashEntryAdr = next)
                {
                    next = HashEntries.getNext(hashEntryAdr);
                    Uns.free(hashEntryAdr);
                }

            table.clear();
        }
        finally
        {
            unlock(wasFirst);
        }
    }


    boolean removeEntry(KeyBuffer key)
    {
        long removeHashEntryAdr = 0L;
        boolean wasFirst = lock();
        try
        {
            long prevEntryAdr = 0L;
            for (long hashEntryAdr = table.getFirst(key.hash());
                 hashEntryAdr != 0L;
                 prevEntryAdr = hashEntryAdr, hashEntryAdr = HashEntries.getNext(hashEntryAdr))
            {
                if (!key.sameKey(hashEntryAdr))
                    continue;

                // remove existing entry

                removeHashEntryAdr = hashEntryAdr;
                removeInternal(hashEntryAdr, prevEntryAdr, key.hash());

                size--;
                removeCount++;

                return true;
            }

            return false;
        }
        finally
        {
            unlock(wasFirst);
            if (removeHashEntryAdr != 0L)
                Uns.free(removeHashEntryAdr);
        }
    }

    private void rehash()
    {
        Table tab = table;
        int tableSize = tab.size();
        if (tableSize > MAX_TABLE_SIZE)
        {
            // already at max hash table size
            return;
        }

        Table newTable = Table.create(tableSize * 2, throwOOME);
        if (newTable == null)
            return;
        long next;

        Hasher hasher = Hasher.create(hashAlgorithm);

        for (int part = 0; part < tableSize; part++)
            for (long hashEntryAdr = tab.getFirst(part);
                 hashEntryAdr != 0L;
                 hashEntryAdr = next) {

                next = HashEntries.getNext(hashEntryAdr);
                HashEntries.setNext(hashEntryAdr, 0L);
                long hash = hasher.hash(hashEntryAdr, HashEntries.ENTRY_OFF_DATA, HashEntries.getKeyLen(hashEntryAdr));
                newTable.addAsHead(hash, hashEntryAdr);
            }

        threshold = (long) ((float) newTable.size() * loadFactor);
        table.release();
        table = newTable;
        rehashes++;
    }

    float loadFactor()
    {
        return loadFactor;
    }

    int hashTableSize()
    {
        return table.size();
    }

    void updateBucketHistogram(EstimatedHistogram hist)
    {
        boolean wasFirst = lock();
        try
        {
            table.updateBucketHistogram(hist);
        }
        finally
        {
            unlock(wasFirst); 
        }
    }

    void getEntryAddresses(int mapSegmentIndex, int nSegments, LongArrayList hashEntryAdrs)
    {
        boolean wasFirst = lock();
        try
        {
            for (; nSegments-- > 0 && mapSegmentIndex < table.size(); mapSegmentIndex++)
                for (long hashEntryAdr = table.getFirst(mapSegmentIndex);
                     hashEntryAdr != 0L;
                     hashEntryAdr = HashEntries.getNext(hashEntryAdr))
                {
                    hashEntryAdrs.add(hashEntryAdr);
                }
        }
        finally
        {
            unlock(wasFirst); 
        }
    }

    static final class Table
    {
        final int mask;
        final long address;
        private boolean released;

        static Table create(int hashTableSize, boolean throwOOME)
        {
            int msz = Ints.checkedCast(Util.BUCKET_ENTRY_LEN * hashTableSize);
            long address = Uns.allocate(msz, throwOOME);
            return address != 0L ? new Table(address, hashTableSize) : null;
        }

        private Table(long address, int hashTableSize)
        {
            this.address = address;
            this.mask = hashTableSize - 1;
            clear();
        }

        void clear()
        {
            // It's important to initialize the hash table memory.
            // (uninitialized memory will cause problems - endless loops, JVM crashes, damaged data, etc)
            Uns.setMemory(address, 0L, Util.BUCKET_ENTRY_LEN * size(), (byte) 0);
        }

        void release()
        {
            Uns.free(address);
            released = true;
        }

        protected void finalize() throws Throwable
        {
            if (!released)
                Uns.free(address);
            super.finalize();
        }

        long getFirst(long hash)
        {
            return Uns.getLong(address, bucketOffset(hash));
        }

        void setFirst(long hash, long hashEntryAdr)
        {
            Uns.putLong(address, bucketOffset(hash), hashEntryAdr);
        }

        long bucketOffset(long hash)
        {
            return bucketIndexForHash(hash) * Util.BUCKET_ENTRY_LEN;
        }

        private int bucketIndexForHash(long hash)
        {
            return (int) (hash & mask);
        }

        void removeLink(long hash, long hashEntryAdr, long prevEntryAdr)
        {
            long next = HashEntries.getNext(hashEntryAdr);

            removeLinkInternal(hash, hashEntryAdr, prevEntryAdr, next);
        }

        void replaceSentinelLink(long hash, long hashEntryAdr, long prevEntryAdr, long newHashEntryAdr)
        {
            HashEntries.setNext(newHashEntryAdr, HashEntries.getNext(hashEntryAdr));

            removeLinkInternal(hash, hashEntryAdr, prevEntryAdr, newHashEntryAdr);
        }

        private void removeLinkInternal(long hash, long hashEntryAdr, long prevEntryAdr, long next)
        {
            long head = getFirst(hash);
            if (head == hashEntryAdr)
            {
                setFirst(hash, next);
            }
            else if (prevEntryAdr != 0L)
            {
                if (prevEntryAdr == -1L)
                {
                    for (long adr = head;
                         adr != 0L;
                         prevEntryAdr = adr, adr = HashEntries.getNext(adr))
                    {
                        if (adr == hashEntryAdr)
                            break;
                    }
                }
                HashEntries.setNext(prevEntryAdr, next);
            }
        }

        void addAsHead(long hash, long hashEntryAdr)
        {
            long head = getFirst(hash);
            HashEntries.setNext(hashEntryAdr, head);
            setFirst(hash, hashEntryAdr);
        }

        int size()
        {
            return mask + 1;
        }

        void updateBucketHistogram(EstimatedHistogram h)
        {
            for (int i = 0; i < size(); i++)
            {
                int len = 0;
                for (long adr = getFirst(i); adr != 0L; adr = HashEntries.getNext(adr))
                    len++;
                h.add(len + 1);
            }
        }
    }

    private void removeInternal(long hashEntryAdr, long prevEntryAdr, long hash) {
        table.removeLink(hash, hashEntryAdr, prevEntryAdr);
    }

    private void add(long hashEntryAdr, long hash)
    {
        table.addAsHead(hash, hashEntryAdr);
    }

    boolean lock()
    {
        if (unlocked)
            return false;

        long t = Thread.currentThread().getId();

        if (t == lockFieldUpdater.get(this))
            return false;
        while (true)
        {
            if (lockFieldUpdater.compareAndSet(this, 0L, t))
                return true;

            // yield control to other thread.
            // Note: we cannot use LockSupport.parkNanos() as that does not
            // provide nanosecond resolution on Windows.
            Thread.yield();
        }
    }

    void unlock(boolean wasFirst)
    {
        if (unlocked || !wasFirst)
            return;

        long t = Thread.currentThread().getId();
        boolean r = lockFieldUpdater.compareAndSet(this, t, 0L);
        assert r;
    }

    @Override
    public String toString()
    {
        return String.valueOf(size);
    }
}
