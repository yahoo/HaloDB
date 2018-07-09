/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import com.google.common.base.MoreObjects;

/**
 * @author Arjun Mannaly
 */
class SegmentStats {

    private final long noOfEntries;
    private final long numberOfChunks;
    private final long numberOfSlots;
    private final long freeListSize;

    public SegmentStats(long noOfEntries, long numberOfChunks, long numberOfSlots, long freeListSize) {
        this.noOfEntries = noOfEntries;
        this.numberOfChunks = numberOfChunks;
        this.numberOfSlots = numberOfSlots;
        this.freeListSize = freeListSize;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper("")
            .add("noOfEntries", noOfEntries)
            .add("numberOfChunks", numberOfChunks)
            .add("numberOfSlots", numberOfSlots)
            .add("freeListSize", freeListSize)
            .toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (!(obj instanceof SegmentStats))
            return false;

        SegmentStats that = (SegmentStats) obj;
        return that.noOfEntries == noOfEntries
               && that.numberOfChunks == numberOfChunks
               && that.numberOfSlots == numberOfSlots
               && that.freeListSize == freeListSize;
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = 31 * result + Long.hashCode(noOfEntries);
        result = 31 * result + Long.hashCode(numberOfChunks);
        result = 31 * result + Long.hashCode(numberOfSlots);
        result = 31 * result + Long.hashCode(freeListSize);

        return result;
    }
}
