/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.google.common.primitives.Longs;

public class RehashTest
{
    @AfterMethod(alwaysRun = true)
    public void deinit()
    {
        Uns.clearUnsDebugForTest();
    }

    @Test
    public void testRehash() throws IOException
    {
        ByteArrayEntrySerializer serializer = ByteArrayEntrySerializer.ofSize(8);
        try (OffHeapHashTable<ByteArrayEntry> cache = OffHeapHashTableBuilder.newBuilder(serializer)
                                                            .hashTableSize(64)
                                                            .segmentCount(4)
                                                            .build())
        {
            for (int i = 0; i < 100000; i++)
                cache.put(Longs.toByteArray(i), serializer.createEntry(8, Longs.toByteArray(i)));

            assertTrue(cache.stats().getRehashCount() > 0);

            for (int i = 0; i < 100000; i++)
            {
                ByteArrayEntry v = cache.get(Longs.toByteArray(i));
                assertEquals(Longs.fromByteArray(v.bytes), i);
            }
        }
    }
}
