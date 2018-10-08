/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import com.oath.halodb.histo.EstimatedHistogram;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This is a {@link OffHeapHashTable} implementation used to validate functionality of
 * {@link OffHeapHashTableImpl} - this implementation is <b>not</b> for production use!
 */
final class CheckOffHeapHashTable<V> implements OffHeapHashTable<V>
{
    private final HashTableValueSerializer<V> valueSerializer;

    private final CheckSegment[] maps;
    private final long maxEntrySize;
    private final int segmentShift;
    private final long segmentMask;
    private final float loadFactor;
    private long putFailCount;
    private final Hasher hasher;

    CheckOffHeapHashTable(OffHeapHashTableBuilder<V> builder)
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

        valueSerializer = builder.getValueSerializer();

        maxEntrySize = builder.getMaxEntrySize();
    }

    public boolean put(byte[] key, V value)
    {
        KeyBuffer keyBuffer = keySource(key);
        byte[] data = value(value);

        if (maxEntrySize > 0L && CheckSegment.sizeOf(keyBuffer, data) > maxEntrySize)
        {
            remove(key);
            putFailCount++;
            return false;
        }

        CheckSegment segment = segment(keyBuffer.hash());
        return segment.put(keyBuffer, data, false, null);
    }

    public boolean addOrReplace(byte[] key, V old, V value)
    {
        KeyBuffer keyBuffer = keySource(key);
        byte[] data = value(value);
        byte[] oldData = value(old);

        if (maxEntrySize > 0L && CheckSegment.sizeOf(keyBuffer, data) > maxEntrySize)
        {
            remove(key);
            putFailCount++;
            return false;
        }

        CheckSegment segment = segment(keyBuffer.hash());
        return segment.put(keyBuffer, data, false, oldData);
    }

    public boolean putIfAbsent(byte[] key, V v)
    {
        KeyBuffer keyBuffer = keySource(key);
        byte[] data = value(v);

        if (maxEntrySize > 0L && CheckSegment.sizeOf(keyBuffer, data) > maxEntrySize)
        {
            remove(key);
            putFailCount++;
            return false;
        }

        CheckSegment segment = segment(keyBuffer.hash());
        return segment.put(keyBuffer, data, true, null);
    }

    public boolean putIfAbsent(byte[] key, V value, long expireAt)
    {
        throw new UnsupportedOperationException();
    }

    public boolean put(byte[] key, V value, long expireAt)
    {
        throw new UnsupportedOperationException();
    }

    public boolean remove(byte[] key)
    {
        KeyBuffer keyBuffer = keySource(key);
        CheckSegment segment = segment(keyBuffer.hash());
        return segment.remove(keyBuffer);
    }

    public void clear()
    {
        for (CheckSegment map : maps)
            map.clear();
    }

    public V get(byte[] key)
    {
        KeyBuffer keyBuffer = keySource(key);
        CheckSegment segment = segment(keyBuffer.hash());
        byte[] value = segment.get(keyBuffer);

        if (value == null)
            return null;

        return valueSerializer.deserialize(ByteBuffer.wrap(value));
    }

    public boolean containsKey(byte[] key)
    {
        KeyBuffer keyBuffer = keySource(key);
        CheckSegment segment = segment(keyBuffer.hash());
        return segment.get(keyBuffer) != null;
    }

    public void resetStatistics()
    {
        for (CheckSegment map : maps)
            map.resetStatistics();
        putFailCount = 0;
    }

    public long size()
    {
        long r = 0;
        for (CheckSegment map : maps)
            r += map.size();
        return r;
    }

    public int[] hashTableSizes()
    {
        // no hash table size info
        return new int[maps.length];
    }

    public SegmentStats[] perSegmentStats() {
        SegmentStats[] stats = new SegmentStats[maps.length];
        for (int i = 0; i < stats.length; i++) {
            CheckSegment map = maps[i];
            stats[i] = new SegmentStats(map.size(), -1, -1, -1);
        }

        return stats;
    }

    public EstimatedHistogram getBucketHistogram()
    {
        throw new UnsupportedOperationException();
    }

    public int segments()
    {
        return maps.length;
    }

    public float loadFactor()
    {
        return loadFactor;
    }

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

    private byte[] value(V value)
    {
        if (value == null) {
            return null;
        }
        ByteBuffer buf = ByteBuffer.allocate(valueSerializer.serializedSize(value));
        valueSerializer.serialize(value, buf);
        return buf.array();
    }
}
