/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

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

import java.nio.ByteBuffer;
import java.util.Arrays;

final class KeyBuffer
{
    final byte[] buffer;
    private long hash;

    KeyBuffer(int size)
    {
        buffer = new byte[size];
    }

    long hash()
    {
        return hash;
    }

    KeyBuffer finish(Hasher hasher)
    {
        hash = hasher.hash(buffer);

        return this;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KeyBuffer keyBuffer = (KeyBuffer) o;

        return buffer.length == keyBuffer.buffer.length && Arrays.equals(keyBuffer.buffer, buffer);
    }

    public int hashCode()
    {
        return (int) hash;
    }

    private static String pad(int val)
    {
        String str = Integer.toHexString(val & 0xff);
        while (str.length() == 1)
            str = '0' + str;
        return str;
    }

    @Override
    public String toString()
    {
        byte[] b = buffer;
        StringBuilder sb = new StringBuilder(b.length * 3);
        for (int ii = 0; ii < b.length; ii++) {
            if (ii % 8 == 0 && ii != 0) sb.append('\n');
            sb.append(pad(b[ii]));
            sb.append(' ');
        }
        return sb.toString();
    }

    ByteBuffer byteBuffer()
    {
        return ByteBuffer.wrap(buffer);
    }

    boolean sameKey(long hashEntryAdr) {
        long serKeyLen = HashEntries.getKeyLen(hashEntryAdr);
        return serKeyLen == buffer.length && compareKey(hashEntryAdr);
    }

    private boolean compareKey(long hashEntryAdr) {
        int blkOff = (int) HashEntries.ENTRY_OFF_DATA;
        int p = 0;
        int endIdx = buffer.length;
        for (; endIdx - p >= 8; p += 8)
            if (Uns.getLong(hashEntryAdr, blkOff + p) != Uns.getLongFromByteArray(buffer, p))
                return false;
        for (; endIdx - p >= 4; p += 4)
            if (Uns.getInt(hashEntryAdr, blkOff + p) != Uns.getIntFromByteArray(buffer, p))
                return false;
        for (; endIdx - p >= 2; p += 2)
            if (Uns.getShort(hashEntryAdr, blkOff + p) != Uns.getShortFromByteArray(buffer, p))
                return false;
        for (; endIdx - p >= 1; p += 1)
            if (Uns.getByte(hashEntryAdr, blkOff + p) != buffer[p])
                return false;

        return true;
    }
}
