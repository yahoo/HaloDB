/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import java.io.IOException;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class LinkedImplTest {
    static ByteArrayEntrySerializer serializer = ByteArrayEntrySerializer.ofSize(33);

    @AfterMethod(alwaysRun = true)
    public void deinit()
    {
        Uns.clearUnsDebugForTest();
    }

    static OffHeapHashTable<?> cache()
    {
        return cache(256);
    }

    static OffHeapHashTable<?> cache(long capacity)
    {
        return cache(capacity, -1);
    }

    static OffHeapHashTable<?> cache(long capacity, int hashTableSize)
    {
        return cache(capacity, hashTableSize, -1, -1);
    }

    static OffHeapHashTable<?> cache(long capacity, int hashTableSize, int segments, long maxEntrySize)
    {
        OffHeapHashTableBuilder<?> builder = OffHeapHashTableBuilder.newBuilder(serializer);
        if (hashTableSize > 0)
            builder.hashTableSize(hashTableSize);
        if (segments > 0)
            builder.segmentCount(segments);
        else
            // use 16 segments by default to prevent differing test behaviour on varying test hardware
            builder.segmentCount(16);

        return builder.build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testExtremeHashTableSize() throws IOException
    {
        OffHeapHashTableBuilder<?> builder = OffHeapHashTableBuilder.newBuilder(serializer).hashTableSize(1 << 30);
        builder.build().close();
    }

}
