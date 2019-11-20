/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

final class HashTableUtil {

// Hash bucket-table

    static final long NON_MEMORY_POOL_BUCKET_ENTRY_LEN = 8;
    static final long MEMORY_POOL_BUCKET_ENTRY_LEN = 5;

    static long allocLen(long keyLen, long valueLen) {
        return NonMemoryPoolHashEntries.ENTRY_OFF_DATA + keyLen + valueLen;
    }

    static int bitNum(int val) {
        return 32 - Integer.numberOfLeadingZeros(val);
    }

    static int roundUpToPowerOf2(int posNum, int maxPower) {
        int max = 1 << maxPower;
        if (posNum >= max) {
            return max;
        }
        return 1 << bitNum(posNum - 1);
    }
}
