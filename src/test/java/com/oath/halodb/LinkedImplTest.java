/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.IOException;

public class LinkedImplTest
{
    @AfterMethod(alwaysRun = true)
    public void deinit()
    {
        Uns.clearUnsDebugForTest();
    }

    static OffHeapHashTable<String> cache()
    {
        return cache(256);
    }

    static OffHeapHashTable<String> cache(long capacity)
    {
        return cache(capacity, -1);
    }

    static OffHeapHashTable<String> cache(long capacity, int hashTableSize)
    {
        return cache(capacity, hashTableSize, -1, -1);
    }

    static OffHeapHashTable<String> cache(long capacity, int hashTableSize, int segments, long maxEntrySize)
    {
        OffHeapHashTableBuilder<String> builder = OffHeapHashTableBuilder.<String>newBuilder()
                                                  .valueSerializer(HashTableTestUtils.stringSerializer)
                                                  .capacity(capacity * HashTableTestUtils.ONE_MB);
        if (hashTableSize > 0)
            builder.hashTableSize(hashTableSize);
        if (segments > 0)
            builder.segmentCount(segments);
        else
            // use 16 segments by default to prevent differing test behaviour on varying test hardware
            builder.segmentCount(16);
        if (maxEntrySize > 0)
            builder.maxEntrySize(maxEntrySize);

        return builder.build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testExtremeHashTableSize() throws IOException
    {
        OffHeapHashTableBuilder<Object> builder = OffHeapHashTableBuilder.newBuilder()
                                                               .hashTableSize(1 << 30);
        builder.build().close();
    }

}
