/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

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
            NonMemoryPoolHashEntries.init(adr);

            assertEquals(Uns.getLong(adr, NonMemoryPoolHashEntries.ENTRY_OFF_NEXT), 0L);

            assertEquals(NonMemoryPoolHashEntries.getNext(adr), 0L);
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

            Uns.putLong(adr, NonMemoryPoolHashEntries.ENTRY_OFF_NEXT, 0x98765432abdffeedL);
            assertEquals(NonMemoryPoolHashEntries.getNext(adr), 0x98765432abdffeedL);

            NonMemoryPoolHashEntries.setNext(adr, 0xfafefcfb23242526L);
            assertEquals(Uns.getLong(adr, NonMemoryPoolHashEntries.ENTRY_OFF_NEXT), 0xfafefcfb23242526L);
            assertEquals(NonMemoryPoolHashEntries.getNext(adr), 0xfafefcfb23242526L);
        }
        finally
        {
            Uns.free(adr);
        }
    }
}
