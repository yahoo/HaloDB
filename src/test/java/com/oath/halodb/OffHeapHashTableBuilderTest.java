/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import org.testng.Assert;
import org.testng.annotations.Test;

public class OffHeapHashTableBuilderTest
{
    ByteArrayEntrySerializer serializer = ByteArrayEntrySerializer.ofSize(3);

    @Test
    public void testHashTableSize() throws Exception
    {
        OffHeapHashTableBuilder<?> builder = OffHeapHashTableBuilder.newBuilder(serializer);
        Assert.assertEquals(builder.getHashTableSize(), 8192);
        builder.hashTableSize(12345);
        Assert.assertEquals(builder.getHashTableSize(), 12345);
    }

    @Test
    public void testChunkSize() throws Exception
    {
        OffHeapHashTableBuilder<?> builder = OffHeapHashTableBuilder.newBuilder(serializer);
        builder.memoryPoolChunkSize(12345);
        Assert.assertEquals(builder.getMemoryPoolChunkSize(), 12345);
    }

    @Test
    public void testSegmentCount() throws Exception
    {
        int cpus = Runtime.getRuntime().availableProcessors();
        int segments = cpus * 2;
        while (Integer.bitCount(segments) != 1)
            segments++;

        OffHeapHashTableBuilder<?> builder = OffHeapHashTableBuilder.newBuilder(serializer);
        Assert.assertEquals(builder.getSegmentCount(), segments);
        builder.segmentCount(12345);
        Assert.assertEquals(builder.getSegmentCount(), 12345);
    }

    @Test
    public void testLoadFactor() throws Exception
    {
        OffHeapHashTableBuilder<?> builder = OffHeapHashTableBuilder.newBuilder(serializer);
        Assert.assertEquals(builder.getLoadFactor(), .75f);
        builder.loadFactor(12345);
        Assert.assertEquals(builder.getLoadFactor(), 12345.0f);
    }

    @Test
    public void testValueSerializer() throws Exception
    {
        OffHeapHashTableBuilder<ByteArrayEntry> builder = OffHeapHashTableBuilder.newBuilder(serializer);
        Assert.assertSame(builder.getEntrySerializer(), serializer);
    }

    @Test
    public void testFixedValueSize() throws Exception {
        OffHeapHashTableBuilder<?> builder = OffHeapHashTableBuilder.newBuilder(serializer);
        builder.build();
    }
}
