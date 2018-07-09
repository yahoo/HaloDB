/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static org.testng.Assert.*;

public class NonMemoryPoolHashEntriesTest
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
            NonMemoryPoolHashEntries.init(5, adr);

            assertEquals(Uns.getLong(adr, NonMemoryPoolHashEntries.ENTRY_OFF_NEXT), 0L);
            assertEquals(Uns.getByte(adr, NonMemoryPoolHashEntries.ENTRY_OFF_KEY_LENGTH), 5);

            assertEquals(NonMemoryPoolHashEntries.getNext(adr), 0L);
            assertEquals(NonMemoryPoolHashEntries.getKeyLen(adr), 5L);
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
        try
        {
            NonMemoryPoolHashEntries.init(11, adr);

            ByteBuffer buffer = ByteBuffer.allocate(11);
            buffer.putInt(0x98765432);
            buffer.putInt(0xabcdabba);
            buffer.put((byte)(0x44 & 0xff));
            buffer.put((byte)(0x55 & 0xff));
            buffer.put((byte)(0x88 & 0xff));

            KeyBuffer key = new KeyBuffer(buffer.array());
            key.finish(Hasher.create(HashAlgorithm.MURMUR3));

            Uns.setMemory(adr, NonMemoryPoolHashEntries.ENTRY_OFF_DATA, 11, (byte) 0);

            assertFalse(key.sameKey(adr));

            Uns.copyMemory(key.buffer, 0, adr, NonMemoryPoolHashEntries.ENTRY_OFF_DATA, 11);
            NonMemoryPoolHashEntries.init(11, adr);

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
            NonMemoryPoolHashEntries.init(5, adr);

            Uns.putLong(adr, NonMemoryPoolHashEntries.ENTRY_OFF_NEXT, 0x98765432abdffeedL);
            assertEquals(NonMemoryPoolHashEntries.getNext(adr), 0x98765432abdffeedL);

            NonMemoryPoolHashEntries.setNext(adr, 0xfafefcfb23242526L);
            assertEquals(Uns.getLong(adr, NonMemoryPoolHashEntries.ENTRY_OFF_NEXT), 0xfafefcfb23242526L);
        }
        finally
        {
            Uns.free(adr);
        }
    }
}
