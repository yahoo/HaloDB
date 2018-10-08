/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import com.google.common.base.Objects;

final class OffHeapHashTableStats {

    private final long hitCount;
    private final long missCount;
    private final long size;
    private final long rehashCount;
    private final long putAddCount;
    private final long putReplaceCount;
    private final long putFailCount;
    private final long removeCount;
    private final SegmentStats[] segmentStats;

    public OffHeapHashTableStats(long hitCount, long missCount,
                                 long size, long rehashCount,
                                 long putAddCount, long putReplaceCount, long putFailCount, long removeCount,
                                 SegmentStats[] segmentStats) {
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.size = size;
        this.rehashCount = rehashCount;
        this.putAddCount = putAddCount;
        this.putReplaceCount = putReplaceCount;
        this.putFailCount = putFailCount;
        this.removeCount = removeCount;
        this.segmentStats = segmentStats;
    }

    public long getRehashCount() {
        return rehashCount;
    }

    public long getHitCount() {
        return hitCount;
    }

    public long getMissCount() {
        return missCount;
    }

    public long getSize() {
        return size;
    }

    public long getPutAddCount() {
        return putAddCount;
    }

    public long getPutReplaceCount() {
        return putReplaceCount;
    }

    public long getPutFailCount() {
        return putFailCount;
    }

    public long getRemoveCount() {
        return removeCount;
    }

    public SegmentStats[] getSegmentStats() {
        return segmentStats;
    }

    public String toString() {
        return Objects.toStringHelper(this)
            .add("hitCount", hitCount)
            .add("missCount", missCount)
            .add("size", size)
            .add("rehashCount", rehashCount)
            .add("put(add/replace/fail)", Long.toString(putAddCount) + '/' + putReplaceCount + '/' + putFailCount)
            .add("removeCount", removeCount)
            .toString();
    }

    private static long maxOf(long[] arr) {
        long r = 0;
        for (long l : arr) {
            if (l > r) {
                r = l;
            }
        }
        return r;
    }

    private static long minOf(long[] arr) {
        long r = Long.MAX_VALUE;
        for (long l : arr) {
            if (l < r) {
                r = l;
            }
        }
        return r;
    }

    private static double avgOf(long[] arr) {
        double r = 0d;
        for (long l : arr) {
            r += l;
        }
        return r / arr.length;
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OffHeapHashTableStats that = (OffHeapHashTableStats) o;

        if (hitCount != that.hitCount) return false;
        if (missCount != that.missCount) return false;
        if (putAddCount != that.putAddCount) return false;
        if (putFailCount != that.putFailCount) return false;
        if (putReplaceCount != that.putReplaceCount) return false;
//        if (rehashCount != that.rehashCount) return false;
        if (removeCount != that.removeCount) return false;
        if (size != that.size) return false;
//        if (totalAllocated != that.totalAllocated) return false;

        return true;
    }

    public int hashCode() {
        int result = (int) (hitCount ^ (hitCount >>> 32));
        result = 31 * result + (int) (missCount ^ (missCount >>> 32));
        result = 31 * result + (int) (size ^ (size >>> 32));
//        result = 31 * result + (int) (rehashCount ^ (rehashCount >>> 32));
        result = 31 * result + (int) (putAddCount ^ (putAddCount >>> 32));
        result = 31 * result + (int) (putReplaceCount ^ (putReplaceCount >>> 32));
        result = 31 * result + (int) (putFailCount ^ (putFailCount >>> 32));
        result = 31 * result + (int) (removeCount ^ (removeCount >>> 32));
//        result = 31 * result + (int) (totalAllocated ^ (totalAllocated >>> 32));
        return result;
    }
}
