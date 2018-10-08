/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import com.google.common.primitives.Ints;
import com.oath.halodb.histo.EstimatedHistogram;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

final class OffHeapHashTableImpl<V> implements OffHeapHashTable<V> {

    private static final Logger logger = LoggerFactory.getLogger(OffHeapHashTableImpl.class);

    private final HashTableValueSerializer<V> valueSerializer;

    private final int fixedValueLength;

    private final List<Segment<V>> segments;
    private final long segmentMask;
    private final int segmentShift;

    private final int segmentCount;

    private volatile long putFailCount;

    private boolean closed;

    private final Hasher hasher;

    OffHeapHashTableImpl(OffHeapHashTableBuilder<V> builder) {
        this.hasher = Hasher.create(builder.getHashAlgorighm());
        this.fixedValueLength = builder.getFixedValueSize();

        // build segments
        if (builder.getSegmentCount() <= 0) {
            throw new IllegalArgumentException("Segment count should be > 0");
        }
        segmentCount = Ints.checkedCast(HashTableUtil.roundUpToPowerOf2(builder.getSegmentCount(), 1 << 30));
        segments = new ArrayList<>(segmentCount);
        for (int i = 0; i < segmentCount; i++) {
            try {
                segments.add(allocateSegment(builder));
            } catch (RuntimeException e) {
                for (; i >= 0; i--) {
                    if (segments.get(i) != null) {
                        segments.get(i).release();
                    }
                }
                throw e;
            }
        }

        // bit-mask for segment part of hash
        int bitNum = HashTableUtil.bitNum(segmentCount) - 1;
        this.segmentShift = 64 - bitNum;
        this.segmentMask = ((long) segmentCount - 1) << segmentShift;

        this.valueSerializer = builder.getValueSerializer();
        if (valueSerializer == null) {
            throw new NullPointerException("valueSerializer == null");
        }

        logger.debug("off-heap index with {} segments created.", segmentCount);
    }

    private Segment<V> allocateSegment(OffHeapHashTableBuilder<V> builder) {
        if (builder.isUseMemoryPool()) {
            return new SegmentWithMemoryPool<>(builder);
        }
        return new SegmentNonMemoryPool<>(builder);
    }

    public V get(byte[] key) {
        if (key == null) {
            throw new NullPointerException();
        }

        KeyBuffer keySource = keySource(key);
        return segment(keySource.hash()).getEntry(keySource);
    }

    public boolean containsKey(byte[] key) {
        if (key == null) {
            throw new NullPointerException();
        }

        KeyBuffer keySource = keySource(key);
        return segment(keySource.hash()).containsEntry(keySource);
    }

    public boolean put(byte[] k, V v) {
        return putInternal(k, v, false, null);
    }

    public boolean addOrReplace(byte[] key, V old, V value) {
        return putInternal(key, value, false, old);
    }

    public boolean putIfAbsent(byte[] k, V v) {
        return putInternal(k, v, true, null);
    }

    private boolean putInternal(byte[] key, V value, boolean ifAbsent, V old) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }

        int valueSize = valueSize(value);
        if (valueSize != fixedValueLength) {
            throw new IllegalArgumentException("value size " + valueSize + " greater than fixed value size " + fixedValueLength);
        }

        if (old != null && valueSize(old) != fixedValueLength) {
            throw new IllegalArgumentException("old value size " + valueSize(old) + " greater than fixed value size " + fixedValueLength);
        }

        if (key.length > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("key size of " + key.length + " exceeds max permitted size of " + Byte.MAX_VALUE);
        }

        long hash = hasher.hash(key);
        return segment(hash).putEntry(key, value, hash, ifAbsent, old);
    }

    private int valueSize(V v) {
        int sz = valueSerializer.serializedSize(v);
        if (sz <= 0) {
            throw new IllegalArgumentException("Illegal value length " + sz);
        }
        return sz;
    }

    public boolean remove(byte[] k) {
        if (k == null) {
            throw new NullPointerException();
        }

        KeyBuffer keySource = keySource(k);
        return segment(keySource.hash()).removeEntry(keySource);
    }

    private Segment<V> segment(long hash) {
        int seg = (int) ((hash & segmentMask) >>> segmentShift);
        return segments.get(seg);
    }

    private KeyBuffer keySource(byte[] key) {
        KeyBuffer keyBuffer = new KeyBuffer(key);
        return keyBuffer.finish(hasher);
    }

    //
    // maintenance
    //

    public void clear() {
        for (Segment map : segments) {
            map.clear();
        }
    }

    //
    // state
    //

    //TODO: remove.
    public void setCapacity(long capacity) {

    }

    public void close() {
        closed = true;
        for (Segment map : segments) {
            map.release();
        }
        Collections.fill(segments, null);

        if (logger.isDebugEnabled()) {
            logger.debug("Closing OHC instance");
        }
    }

    //
    // statistics and related stuff
    //

    public void resetStatistics() {
        for (Segment map : segments) {
            map.resetStatistics();
        }
        putFailCount = 0;
    }

    public OffHeapHashTableStats stats() {
        long hitCount = 0, missCount = 0, size = 0,
            freeCapacity = 0, rehashes = 0, putAddCount = 0, putReplaceCount = 0, removeCount = 0;
        for (Segment map : segments) {
            hitCount += map.hitCount();
            missCount += map.missCount();
            size += map.size();
            rehashes += map.rehashes();
            putAddCount += map.putAddCount();
            putReplaceCount += map.putReplaceCount();
            removeCount += map.removeCount();
        }

        return new OffHeapHashTableStats(
            hitCount,
            missCount,
            size,
            rehashes,
            putAddCount,
            putReplaceCount,
            putFailCount,
            removeCount,
            perSegmentStats());
    }

    public long size() {
        long size = 0L;
        for (Segment map : segments) {
            size += map.size();
        }
        return size;
    }

    public int segments() {
        return segments.size();
    }

    public float loadFactor() {
        return segments.get(0).loadFactor();
    }

    public int[] hashTableSizes() {
        int[] r = new int[segments.size()];
        for (int i = 0; i < segments.size(); i++) {
            r[i] = segments.get(i).hashTableSize();
        }
        return r;
    }

    public long[] perSegmentSizes() {
        long[] r = new long[segments.size()];
        for (int i = 0; i < segments.size(); i++) {
            r[i] = segments.get(i).size();
        }
        return r;
    }

    public SegmentStats[] perSegmentStats() {
        SegmentStats[] stats = new SegmentStats[segments.size()];
        for (int i = 0; i < stats.length; i++) {
            Segment<V> map = segments.get(i);
            stats[i] = new SegmentStats(map.size(), map.numberOfChunks(), map.numberOfSlots(), map.freeListSize());
        }

        return stats;
    }

    public EstimatedHistogram getBucketHistogram() {
        EstimatedHistogram hist = new EstimatedHistogram();
        for (Segment map : segments) {
            map.updateBucketHistogram(hist);
        }

        long[] offsets = hist.getBucketOffsets();
        long[] buckets = hist.getBuckets(false);

        for (int i = buckets.length - 1; i > 0; i--) {
            if (buckets[i] != 0L) {
                offsets = Arrays.copyOf(offsets, i + 2);
                buckets = Arrays.copyOf(buckets, i + 3);
                System.arraycopy(offsets, 0, offsets, 1, i + 1);
                System.arraycopy(buckets, 0, buckets, 1, i + 2);
                offsets[0] = 0L;
                buckets[0] = 0L;
                break;
            }
        }

        for (int i = 0; i < offsets.length; i++) {
            offsets[i]--;
        }

        return new EstimatedHistogram(offsets, buckets);
    }

    public String toString() {
        return getClass().getSimpleName() + " ,segments=" + segments.size();
    }
}
