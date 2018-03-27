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

package amannaly.cache.linked;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

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
