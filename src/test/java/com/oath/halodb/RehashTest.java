/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import com.google.common.primitives.Longs;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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
        try (OffHeapHashTable<byte[]> cache = OffHeapHashTableBuilder.<byte[]>newBuilder()
                                                            .valueSerializer(HashTableTestUtils.byteArraySerializer)
                                                            .hashTableSize(64)
                                                            .segmentCount(4)
                                                            .capacity(512 * 1024 * 1024)
                                                            .fixedValueSize(8)
                                                            .build())
        {
            for (int i = 0; i < 100000; i++)
                cache.put(Longs.toByteArray(i), Longs.toByteArray(i));

            assertTrue(cache.stats().getRehashCount() > 0);

            for (int i = 0; i < 100000; i++)
            {
                byte[] v = cache.get(Longs.toByteArray(i));
                assertEquals(Longs.fromByteArray(v), i);
            }
        }
    }
}
