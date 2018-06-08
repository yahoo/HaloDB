/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb.cache.linked;

final class Util
{

// Hash bucket-table

    // total memory required for a hash-partition
    static final long BUCKET_ENTRY_LEN = 8;

// Compressed entries header

    // 'OHCC'
    static final int HEADER_COMPRESSED = 0x4f484343;
    // 'OHCC' reversed
    static final int HEADER_COMPRESSED_WRONG = 0x4343484f;
    // 'OHCE'
    static final int HEADER_ENTRIES = 0x4f484345;
    // 'OHCE' reversed
    static final int HEADER_ENTRIES_WRONG = 0x4543484f;
    // 'OHCK'
    static final int HEADER_KEYS = 0x4f48434b;
    // 'OHCK' reversed
    static final int HEADER_KEYS_WRONG = 0x4b43484f;


    static long allocLen(long keyLen, long valueLen)
    {
        return HashEntries.ENTRY_OFF_DATA + keyLen + valueLen;
    }

    static int bitNum(long val)
    {
        int bit = 0;
        for (; val != 0L; bit++)
            val >>>= 1;
        return bit;
    }

    static long roundUpToPowerOf2(long number, long max)
    {
        return number >= max
               ? max
               : (number > 1) ? Long.highestOneBit((number - 1) << 1) : 1;
    }
}
