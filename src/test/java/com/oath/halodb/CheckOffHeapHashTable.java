/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import com.oath.halodb.histo.EstimatedHistogram;

/**
 * This is a {@link OffHeapHashTable} implementation used to validate functionality of
 * {@link OffHeapHashTableImpl} - this implementation is <b>not</b> for production use!
 */
final class CheckOffHeapHashTable<E extends HashEntry> implements OffHeapHashTable<E>
{
    private final HashEntrySerializer<E> serializer;

    private final CheckSegment[] maps;
    private final int segmentShift;
    private final long segmentMask;
    private final float loadFactor;
    private long putFailCount;
    private final Hasher hasher;

    CheckOffHeapHashTable(OffHeapHashTableBuilder<E> builder)
    {
        loadFactor = builder.getLoadFactor();
        hasher = Hasher.create(builder.getHashAlgorighm());

        int segments = builder.getSegmentCount();
        int bitNum = HashTableUtil.bitNum(segments) - 1;
        this.segmentShift = 64 - bitNum;
        this.segmentMask = ((long) segments - 1) << segmentShift;

        maps = new CheckSegment[segments];
        for (int i = 0; i < maps.length; i++)
            maps[i] = new CheckSegment(builder.getHashTableSize(), builder.getLoadFactor());

        serializer = builder.getEntrySerializer();
    }

    @Override
    public boolean put(byte[] key, E entry)
    {
        KeyBuffer keyBuffer = keySource(key);
        byte[] data = entry(entry);

        CheckSegment segment = segment(keyBuffer.hash());
        return segment.put(keyBuffer, data, false, null);
    }

    @Override
    public boolean addOrReplace(byte[] key, E old, E entry)
    {
        KeyBuffer keyBuffer = keySource(key);
        byte[] data = entry(entry);
        byte[] oldData = entry(old);

        CheckSegment segment = segment(keyBuffer.hash());
        return segment.put(keyBuffer, data, false, oldData);
    }

    @Override
    public boolean putIfAbsent(byte[] key, E v)
    {
        KeyBuffer keyBuffer = keySource(key);
        byte[] data = entry(v);

        CheckSegment segment = segment(keyBuffer.hash());
        return segment.put(keyBuffer, data, true, null);
    }

    public boolean putIfAbsent(byte[] key, E entry, long expireAt)
    {
        throw new UnsupportedOperationException();
    }

    public boolean put(byte[] key, E entry, long expireAt)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(byte[] key)
    {
        KeyBuffer keyBuffer = keySource(key);
        CheckSegment segment = segment(keyBuffer.hash());
        return segment.remove(keyBuffer);
    }

    @Override
    public void clear()
    {
        for (CheckSegment map : maps)
            map.clear();
    }

    @Override
    public E get(byte[] key)
    {
        KeyBuffer keyBuffer = keySource(key);
        CheckSegment segment = segment(keyBuffer.hash());
        byte[] entry = segment.get(keyBuffer);

        if (entry == null) {
            return null;
        }
        int entryLen = serializer.entrySize();
        long adr = Uns.allocate(entryLen);
        try {
            Uns.copyMemory(entry, 0, adr, 0, entryLen);
            return serializer.deserialize(adr, adr + serializer.sizesSize());
        } finally {
            Uns.free(adr);
        }
    }

    @Override
    public boolean containsKey(byte[] key)
    {
        KeyBuffer keyBuffer = keySource(key);
        CheckSegment segment = segment(keyBuffer.hash());
        return segment.get(keyBuffer) != null;
    }

    @Override
    public void resetStatistics()
    {
        for (CheckSegment map : maps)
            map.resetStatistics();
        putFailCount = 0;
    }

    @Override
    public long size()
    {
        long r = 0;
        for (CheckSegment map : maps)
            r += map.size();
        return r;
    }

    @Override
    public int[] hashTableSizes()
    {
        // no hash table size info
        return new int[maps.length];
    }

    @Override
    public SegmentStats[] perSegmentStats() {
        SegmentStats[] stats = new SegmentStats[maps.length];
        for (int i = 0; i < stats.length; i++) {
            CheckSegment map = maps[i];
            stats[i] = new SegmentStats(map.size(), -1, -1, -1);
        }

        return stats;
    }

    @Override
    public EstimatedHistogram getBucketHistogram()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int segments()
    {
        return maps.length;
    }

    @Override
    public float loadFactor()
    {
        return loadFactor;
    }

    @Override
    public OffHeapHashTableStats stats()
    {
        return new OffHeapHashTableStats(
                               hitCount(),
                               missCount(),
                               size(),
                               -1L,
                               putAddCount(),
                               putReplaceCount(),
                               putFailCount,
                               removeCount(),
                               perSegmentStats()
        );
    }

    private long putAddCount()
    {
        long putAddCount = 0L;
        for (CheckSegment map : maps)
            putAddCount += map.putAddCount;
        return putAddCount;
    }

    private long putReplaceCount()
    {
        long putReplaceCount = 0L;
        for (CheckSegment map : maps)
            putReplaceCount += map.putReplaceCount;
        return putReplaceCount;
    }

    private long removeCount()
    {
        long removeCount = 0L;
        for (CheckSegment map : maps)
            removeCount += map.removeCount;
        return removeCount;
    }

    private long hitCount()
    {
        long hitCount = 0L;
        for (CheckSegment map : maps)
            hitCount += map.hitCount;
        return hitCount;
    }

    private long missCount()
    {
        long missCount = 0L;
        for (CheckSegment map : maps)
            missCount += map.missCount;
        return missCount;
    }

    @Override
    public void close()
    {
        clear();
    }

    //
    //
    //

    private CheckSegment segment(long hash)
    {
        int seg = (int) ((hash & segmentMask) >>> segmentShift);
        return maps[seg];
    }

    KeyBuffer keySource(byte[] key) {
        KeyBuffer keyBuffer = new KeyBuffer(key);
        return keyBuffer.finish(hasher);
    }

    private byte[] entry(E entry)
    {
        if (entry == null) {
            return null;
        }
        int entryLen = serializer.entrySize();
        long adr = Uns.allocate(entryLen);
        try {
            entry.serializeSizes(adr);
            entry.serializeLocation(adr + serializer.sizesSize());
            byte[] out = new byte[entryLen];
            Uns.copyMemory(adr, 0, out, 0, entryLen);
            return out;
        } finally {
            Uns.free(adr);
        }
    }
}
