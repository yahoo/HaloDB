/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

/**
 * Represents the address of an entry in the memory pool. It will have two components: the index of the chunk which
 * contains the entry and the offset within the chunk.
 */
class MemoryPoolAddress {

    static final MemoryPoolAddress empty = new MemoryPoolAddress((byte)-1, -1);

    final byte chunkIndex;
    final int chunkOffset;

    MemoryPoolAddress(byte chunkIndex, int chunkOffset) {
        this.chunkIndex = chunkIndex;
        this.chunkOffset = chunkOffset;
    }

    final boolean isEmpty() {
        return chunkIndex == -1 & chunkOffset == -1;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof MemoryPoolAddress)) {
            return false;
        }
        MemoryPoolAddress m = (MemoryPoolAddress) o;
        return m.chunkIndex == chunkIndex && m.chunkOffset == chunkOffset;
    }

    @Override
    public int hashCode() {
        return 31 * ((31 * chunkIndex) + chunkOffset);
    }
}
