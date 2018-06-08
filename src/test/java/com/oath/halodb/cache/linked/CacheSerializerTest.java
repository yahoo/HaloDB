/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb.cache.linked;

import com.oath.halodb.ByteArraySerializer;
import com.google.common.primitives.Longs;
import com.oath.halodb.cache.OHCacheBuilder;
import com.oath.halodb.cache.CloseableIterator;
import com.oath.halodb.cache.OHCache;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.IOException;

public class CacheSerializerTest
{
    @AfterMethod(alwaysRun = true)
    public void deinit()
    {
        Uns.clearUnsDebugForTest();
    }

    @Test
    public void testFailingKeySerializer() throws IOException, InterruptedException
    {
        try (OHCache<Integer, byte[]> cache = OHCacheBuilder.<Integer, byte[]>newBuilder()
                                                            .keySerializer(TestUtils.intSerializerFailSerialize)
                                                            .valueSerializer(new ByteArraySerializer())
                                                            .capacity(512L * 1024 * 1024)
                                                            .fixedValueSize(8)
                                                            .build())
        {
            try
            {
                cache.put(1, Longs.toByteArray(1));
                Assert.fail();
            }
            catch (RuntimeException ignored)
            {
                // ok
            }

            try
            {
                cache.get(1);
                Assert.fail();
            }
            catch (RuntimeException ignored)
            {
                // ok
            }

            try
            {
                cache.containsKey(1);
                Assert.fail();
            }
            catch (RuntimeException ignored)
            {
                // ok
            }

            try
            {
                cache.putIfAbsent(1, Longs.toByteArray(1));
                Assert.fail();
            }
            catch (RuntimeException ignored)
            {
                // ok
            }

            try
            {
                cache.addOrReplace(1, Longs.toByteArray(1), Longs.toByteArray(2));
                Assert.fail();
            }
            catch (RuntimeException ignored)
            {
                // ok
            }
        }
    }

    @Test
    public void testFailingKeySerializerInKeyIterator() throws IOException, InterruptedException
    {
        try (OHCache<Integer, byte[]> cache = OHCacheBuilder.<Integer, byte[]>newBuilder()
                                                            .keySerializer(TestUtils.intSerializerFailDeserialize)
                                                            .valueSerializer(new ByteArraySerializer())
                                                            .capacity(512L * 1024 * 1024)
                                                            .fixedValueSize(8)
                                                            .build())
        {
            cache.put(1, Longs.toByteArray(1));
            cache.put(2, Longs.toByteArray(1));
            cache.put(3, Longs.toByteArray(1));

            try
            {
                try (CloseableIterator<Integer> keyIter = cache.keyIterator())
                {
                    while (keyIter.hasNext())
                        keyIter.next();
                }
                Assert.fail();
            }
            catch (RuntimeException ignored)
            {
                // ok
            }

        }
    }

    @Test
    public void testFailingValueSerializerOnPut() throws IOException, InterruptedException
    {
        try (OHCache<Integer, byte[]> cache = OHCacheBuilder.<Integer, byte[]>newBuilder()
                                                            .keySerializer(TestUtils.intSerializer)
                                                            .valueSerializer(TestUtils.byteArraySerializerFailSerialize)
                                                            .capacity(512L * 1024 * 1024)
                                                            .fixedValueSize(8)
                                                            .build())
        {
            try
            {
                cache.put(1, Longs.toByteArray(1));
                Assert.fail();
            }
            catch (RuntimeException ignored)
            {
                // ok
            }

            try
            {
                cache.putIfAbsent(1, Longs.toByteArray(1));
                Assert.fail();
            }
            catch (RuntimeException ignored)
            {
                // ok
            }

            try
            {
                cache.addOrReplace(1, Longs.toByteArray(1), Longs.toByteArray(2));
                Assert.fail();
            }
            catch (RuntimeException ignored)
            {
                // ok
            }

        }
    }
}
