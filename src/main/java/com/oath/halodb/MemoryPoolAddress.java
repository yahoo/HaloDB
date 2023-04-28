/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

/**
 * Represents the address of an entry in the memory pool.
 *
 *  1 byte  -- chunkIndex as an int between 0 and 255, valid chunks are 1 to 255, 0 indicates an empty address
 *  3 bytes -- slot as an int between 0 and 2^24-1  (16.77 million).
 *
 *  With slots using 8 byte 'fixedKeyLength', each slot is 33 bytes, and so each chunk in the memory pool
 *  could hold over 550MB of key data and metadata.  There can be 255 slots, so each segment can fit over
 *  141GB of data in RAM, and there is typically at least 16 segments.
 */
interface MemoryPoolAddress {
    int ADDRESS_SIZE = 4;
    int MAX_NUMBER_OF_SLOTS = (1 << 24) - 1;

    int empty = 0;

    static int encode(int chunkIndex, int slot) {
        if ((chunkIndex >>> 8) != 0) {
            throw new IllegalArgumentException("Invalid chunk index, must be within [0,255], but was: " + chunkIndex);
        }
        if ((slot & 0xFF00_0000) != 0) {
            throw new IllegalArgumentException("Invalid memory pool slot, must be within [0,2^24)" + slot);
        }
        return chunkIndex << 24 | slot & 0x00FF_FFFF;
    }

    /** Always between 0 and (2^24 -1) **/
    static int slot(int memoryPoolAddress) {
        return memoryPoolAddress & 0x00FF_FFFF;
    }

    /** Always between 0 and 255 **/
    static int chunkIndex(int memoryPoolAddress) {
        return memoryPoolAddress >>> 24;
    }

    static boolean isEmpty(int memoryPoolAddress) {
        return memoryPoolAddress == 0;
    }

    static boolean nonEmpty(int memoryPoolAddress) {
        return memoryPoolAddress != 0;
    }
}
