/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oath.halodb.histo.EstimatedHistogram;

final class OffHeapHashTableImpl<E extends HashEntry> implements OffHeapHashTable<E> {

    private static final Logger logger = LoggerFactory.getLogger(OffHeapHashTableImpl.class);

    private final HashEntrySerializer<E> serializer;

    private final Segment<E>[] segments;
    private final long segmentMask;
    private final int segmentShift;

    private final int segmentCount;

    private volatile long putFailCount;

    private boolean closed = false;

    private final Hasher hasher;

    OffHeapHashTableImpl(OffHeapHashTableBuilder<E> builder) {
        this.hasher = Hasher.create(builder.getHashAlgorighm());

        // build segments
        if (builder.getSegmentCount() <= 0) {
            throw new IllegalArgumentException("Segment count should be > 0");
        }
        segmentCount = HashTableUtil.roundUpToPowerOf2(builder.getSegmentCount(), 30);
        segments = new Segment[segmentCount];
        for (int i = 0; i < segmentCount; i++) {
            try {
                segments[i] = (allocateSegment(builder));
            } catch (RuntimeException e) {
                for (; i >= 0; i--) {
                    if (segments[i] != null) {
                        segments[i].release();
                    }
                }
                throw e;
            }
        }

        // bit-mask for segment part of hash
        int bitNum = HashTableUtil.bitNum(segmentCount) - 1;
        this.segmentShift = 64 - bitNum;
        this.segmentMask = ((long) segmentCount - 1) << segmentShift;

        this.serializer = builder.getEntrySerializer();
        if (serializer == null) {
            throw new NullPointerException("serializer == null");
        }

        logger.debug("off-heap index with {} segments created.", segmentCount);
    }

    private Segment<E> allocateSegment(OffHeapHashTableBuilder<E> builder) {
        if (builder.isUseMemoryPool()) {
            return new SegmentWithMemoryPool<>(builder);
        }
        return new SegmentNonMemoryPool<>(builder);
    }

    @Override
    public E get(byte[] key) {
        if (key == null) {
            throw new NullPointerException();
        }

        KeyBuffer keySource = keySource(key);
        return segment(keySource.hash()).getEntry(keySource);
    }

    @Override
    public boolean containsKey(byte[] key) {
        if (key == null) {
            throw new NullPointerException();
        }

        KeyBuffer keySource = keySource(key);
        return segment(keySource.hash()).containsEntry(keySource);
    }

    @Override
    public boolean put(byte[] k, E v) {
        return putInternal(k, v, false, null);
    }

    @Override
    public boolean addOrReplace(byte[] key, E old, E entry) {
        return putInternal(key, entry, false, old);
    }

    @Override
    public boolean putIfAbsent(byte[] k, E v) {
        return putInternal(k, v, true, null);
    }

    private boolean putInternal(byte[] key, E entry, boolean ifAbsent, E old) {
        if (key == null || entry == null) {
            throw new NullPointerException();
        }
        if (key.length > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("key size of " + key.length + " exceeds max permitted size of " + Byte.MAX_VALUE);
        }
        serializer.validateSize(entry);
        if (old != null) {
            serializer.validateSize(entry);
        }

        Utils.validateKeySize(key.length);

        long hash = hasher.hash(key);
        return segment(hash).putEntry(key, entry, hash, ifAbsent, old);
    }

    @Override
    public boolean remove(byte[] k) {
        if (k == null) {
            throw new NullPointerException();
        }

        KeyBuffer keySource = keySource(k);
        return segment(keySource.hash()).removeEntry(keySource);
    }

    private Segment<E> segment(long hash) {
        int seg = (int) ((hash & segmentMask) >>> segmentShift);
        return segments[seg];
    }

    private KeyBuffer keySource(byte[] key) {
        KeyBuffer keyBuffer = new KeyBuffer(key);
        return keyBuffer.finish(hasher);
    }

    //
    // maintenance
    //

    @Override
    public void clear() {
        for (Segment<E> map : segments) {
            map.clear();
        }
    }

    //
    // state
    //

    //TODO: remove.
    public void setCapacity(long capacity) {

    }

    @Override
    public void close() {
        if (closed) {
          return;
        }
        closed = true;
        for (Segment<E> map : segments) {
            map.release();
        }
        Arrays.fill(segments, null);

        if (logger.isDebugEnabled()) {
            logger.debug("Closing OHC instance");
        }
    }

    //
    // statistics and related stuff
    //

    @Override
    public void resetStatistics() {
        for (Segment<E> map : segments) {
            map.resetStatistics();
        }
        putFailCount = 0;
    }

    @Override
    public OffHeapHashTableStats stats() {
        long hitCount = 0, missCount = 0, size = 0,
            rehashes = 0, putAddCount = 0, putReplaceCount = 0, removeCount = 0;
        for (Segment<E> map : segments) {
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

    @Override
    public long size() {
        long size = 0L;
        for (Segment<E> map : segments) {
            size += map.size();
        }
        return size;
    }

    @Override
    public int segments() {
        return segments.length;
    }

    @Override
    public float loadFactor() {
        return segments[0].loadFactor();
    }

    @Override
    public int[] hashTableSizes() {
        int[] r = new int[segments.length];
        for (int i = 0; i < segments.length; i++) {
            r[i] = segments[i].hashTableSize();
        }
        return r;
    }

    public long[] perSegmentSizes() {
        long[] r = new long[segments.length];
        for (int i = 0; i < segments.length; i++) {
            r[i] = segments[i].size();
        }
        return r;
    }

    @Override
    public SegmentStats[] perSegmentStats() {
        SegmentStats[] stats = new SegmentStats[segments.length];
        for (int i = 0; i < stats.length; i++) {
            Segment<E> map = segments[i];
            stats[i] = new SegmentStats(map.size(), map.numberOfChunks(), map.numberOfSlots(), map.freeListSize());
        }

        return stats;
    }

    @Override
    public EstimatedHistogram getBucketHistogram() {
        EstimatedHistogram hist = new EstimatedHistogram();
        for (Segment<E> map : segments) {
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

    @Override
    public String toString() {
        return getClass().getSimpleName() + " ,segments=" + segments.length;
    }
}
