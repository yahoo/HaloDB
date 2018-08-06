/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.HashSet;

public class OffHeapHashTableBuilderTest
{
    @AfterMethod
    public void clearProperties()
    {
        for (Object k : new HashSet(System.getProperties().keySet()))
        {
            String key = (String)k;
            if (key.startsWith("org.caffinitas.ohc."))
                System.getProperties().remove(key);
        }
    }

    @Test
    public void testHashTableSize() throws Exception
    {
        OffHeapHashTableBuilder<String> builder = OffHeapHashTableBuilder.newBuilder();
        Assert.assertEquals(builder.getHashTableSize(), 8192);
        builder.hashTableSize(12345);
        Assert.assertEquals(builder.getHashTableSize(), 12345);

        System.setProperty("org.caffinitas.ohc.hashTableSize", "98765");
        builder = OffHeapHashTableBuilder.newBuilder();
        Assert.assertEquals(builder.getHashTableSize(), 98765);
    }

    @Test
    public void testChunkSize() throws Exception
    {
        OffHeapHashTableBuilder<String> builder = OffHeapHashTableBuilder.newBuilder();
        builder.memoryPoolChunkSize(12345);
        Assert.assertEquals(builder.getMemoryPoolChunkSize(), 12345);
    }

    @Test
    public void testCapacity() throws Exception
    {
        int cpus = Runtime.getRuntime().availableProcessors();

        OffHeapHashTableBuilder<String> builder = OffHeapHashTableBuilder.newBuilder();
        Assert.assertEquals(builder.getCapacity(), Math.min(cpus * 16, 64) * 1024 * 1024);
        builder.capacity(12345);
        Assert.assertEquals(builder.getCapacity(), 12345);

        System.setProperty("org.caffinitas.ohc.capacity", "98765");
        builder = OffHeapHashTableBuilder.newBuilder();
        Assert.assertEquals(builder.getCapacity(), 98765);
    }

    @Test
    public void testSegmentCount() throws Exception
    {
        int cpus = Runtime.getRuntime().availableProcessors();
        int segments = cpus * 2;
        while (Integer.bitCount(segments) != 1)
            segments++;

        OffHeapHashTableBuilder<String> builder = OffHeapHashTableBuilder.newBuilder();
        Assert.assertEquals(builder.getSegmentCount(), segments);
        builder.segmentCount(12345);
        Assert.assertEquals(builder.getSegmentCount(), 12345);

        System.setProperty("org.caffinitas.ohc.segmentCount", "98765");
        builder = OffHeapHashTableBuilder.newBuilder();
        Assert.assertEquals(builder.getSegmentCount(), 98765);
    }

    @Test
    public void testLoadFactor() throws Exception
    {
        OffHeapHashTableBuilder<String> builder = OffHeapHashTableBuilder.newBuilder();
        Assert.assertEquals(builder.getLoadFactor(), .75f);
        builder.loadFactor(12345);
        Assert.assertEquals(builder.getLoadFactor(), 12345.0f);

        System.setProperty("org.caffinitas.ohc.loadFactor", "98765");
        builder = OffHeapHashTableBuilder.newBuilder();
        Assert.assertEquals(builder.getLoadFactor(), 98765.0f);
    }

    @Test
    public void testMaxEntrySize() throws Exception
    {
        OffHeapHashTableBuilder<String> builder = OffHeapHashTableBuilder.newBuilder();
        Assert.assertEquals(builder.getMaxEntrySize(), 0L);
        builder.maxEntrySize(12345);
        Assert.assertEquals(builder.getMaxEntrySize(), 12345);

        System.setProperty("org.caffinitas.ohc.maxEntrySize", "98765");
        builder = OffHeapHashTableBuilder.newBuilder();
        Assert.assertEquals(builder.getMaxEntrySize(), 98765);
    }

    @Test
    public void testValueSerializer() throws Exception
    {
        OffHeapHashTableBuilder<String> builder = OffHeapHashTableBuilder.newBuilder();
        Assert.assertNull(builder.getValueSerializer());

        HashTableValueSerializer<String> inst = new HashTableValueSerializer<String>()
        {
            public void serialize(String s, ByteBuffer out)
            {

            }

            public String deserialize(ByteBuffer in)
            {
                return null;
            }

            public int serializedSize(String s)
            {
                return 0;
            }
        };
        builder.valueSerializer(inst);
        Assert.assertSame(builder.getValueSerializer(), inst);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*Need to set fixedValueSize*")
    public void testFixedValueSize() throws Exception {
        OffHeapHashTableBuilder<String> builder = OffHeapHashTableBuilder.newBuilder();
        builder.build();
    }
}
