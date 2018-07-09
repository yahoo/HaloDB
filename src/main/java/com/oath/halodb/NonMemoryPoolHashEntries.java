/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

/**
 * Encapsulates access to hash entries.
 */
final class NonMemoryPoolHashEntries {

    // offset of next hash entry in a hash bucket (8 bytes, long)
    static final long ENTRY_OFF_NEXT = 0;

    // offset of key length (1 bytes, byte)
    static final long ENTRY_OFF_KEY_LENGTH = 8;

    // offset of data in first block
    static final long ENTRY_OFF_DATA = 9;

    static void init(int keyLen, long hashEntryAdr) {
        setNext(hashEntryAdr, 0L);
        Uns.putByte(hashEntryAdr, ENTRY_OFF_KEY_LENGTH, (byte) keyLen);
    }

    static long getNext(long hashEntryAdr) {
        return hashEntryAdr != 0L ? Uns.getLong(hashEntryAdr, ENTRY_OFF_NEXT) : 0L;
    }

    static void setNext(long hashEntryAdr, long nextAdr) {
        if (hashEntryAdr == nextAdr) {
            throw new IllegalArgumentException();
        }
        if (hashEntryAdr != 0L) {
            Uns.putLong(hashEntryAdr, ENTRY_OFF_NEXT, nextAdr);
        }
    }

    static int getKeyLen(long hashEntryAdr) {
        return Uns.getByte(hashEntryAdr, ENTRY_OFF_KEY_LENGTH);
    }
}
