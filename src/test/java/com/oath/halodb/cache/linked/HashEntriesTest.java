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
package com.oath.halodb.cache.linked;

import com.oath.halodb.cache.HashAlgorithm;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static org.testng.Assert.*;

public class HashEntriesTest
{
    @AfterMethod(alwaysRun = true)
    public void deinit()
    {
        Uns.clearUnsDebugForTest();
    }

    static final long MIN_ALLOC_LEN = 128;

    @Test
    public void testInit() throws Exception
    {
        long adr = Uns.allocate(MIN_ALLOC_LEN);
        try
        {
            HashEntries.init(5, adr);

            assertEquals(Uns.getLong(adr, HashEntries.ENTRY_OFF_NEXT), 0L);
            assertEquals(Uns.getByte(adr, HashEntries.ENTRY_OFF_KEY_LENGTH), 5);

            assertEquals(HashEntries.getNext(adr), 0L);
            assertEquals(HashEntries.getKeyLen(adr), 5L);
        }
        finally
        {
            Uns.free(adr);
        }
    }

    @Test
    public void testCompareKey() throws Exception
    {
        long adr = Uns.allocate(MIN_ALLOC_LEN);
        KeyBuffer key = new KeyBuffer(11);
        try
        {
            HashEntries.init(11, adr);

            ByteBuffer keyBuffer = key.byteBuffer();
            keyBuffer.putInt(0x98765432);
            keyBuffer.putInt(0xabcdabba);
            keyBuffer.put((byte)(0x44 & 0xff));
            keyBuffer.put((byte)(0x55 & 0xff));
            keyBuffer.put((byte)(0x88 & 0xff));
            key.finish(Hasher.create(HashAlgorithm.MURMUR3));

            Uns.setMemory(adr, HashEntries.ENTRY_OFF_DATA, 11, (byte) 0);

            assertFalse(key.sameKey(adr));

            Uns.copyMemory(key.buffer, 0, adr, HashEntries.ENTRY_OFF_DATA, 11);
            HashEntries.init(11, adr);

            assertTrue(key.sameKey(adr));
        }
        finally
        {
            Uns.free(adr);
        }
    }

    @Test
    public void testGetSetNext() throws Exception
    {
        long adr = Uns.allocate(MIN_ALLOC_LEN);
        try
        {
            Uns.setMemory(adr, 0, MIN_ALLOC_LEN, (byte) 0);
            HashEntries.init(5, adr);

            Uns.putLong(adr, HashEntries.ENTRY_OFF_NEXT, 0x98765432abdffeedL);
            assertEquals(HashEntries.getNext(adr), 0x98765432abdffeedL);

            HashEntries.setNext(adr, 0xfafefcfb23242526L);
            assertEquals(Uns.getLong(adr, HashEntries.ENTRY_OFF_NEXT), 0xfafefcfb23242526L);
        }
        finally
        {
            Uns.free(adr);
        }
    }
}
