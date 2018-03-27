/*
 *      Copyright (C) 2014 Robert Stupp, Koeln, Germany, robert-stupp.de
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

/*
 * Modified for HaloDB.
 */

package com.oath.halodb.cache.linked;

/**
 * Encapsulates access to hash entries.
 */
final class HashEntries
{

    // offset of next hash entry in a hash bucket (8 bytes, long)
    static final long ENTRY_OFF_NEXT = 0;
    
    // offset of key length (1 bytes, byte)
    static final long ENTRY_OFF_KEY_LENGTH = 8;

    // offset of data in first block
    static final long ENTRY_OFF_DATA = 9;

    static void init(int keyLen, long hashEntryAdr) {
        setNext(hashEntryAdr, 0L);
        Uns.putByte(hashEntryAdr, ENTRY_OFF_KEY_LENGTH, (byte)keyLen);
    }

    static long getNext(long hashEntryAdr) {
        return hashEntryAdr != 0L ? Uns.getLong(hashEntryAdr, ENTRY_OFF_NEXT) : 0L;
    }

    static void setNext(long hashEntryAdr, long nextAdr) {
        if (hashEntryAdr == nextAdr)
            throw new IllegalArgumentException();
        if (hashEntryAdr != 0L)
            Uns.putLong(hashEntryAdr, ENTRY_OFF_NEXT, nextAdr);
    }

    static int getKeyLen(long hashEntryAdr) {
        return Uns.getByte(hashEntryAdr, ENTRY_OFF_KEY_LENGTH);
    }
}
