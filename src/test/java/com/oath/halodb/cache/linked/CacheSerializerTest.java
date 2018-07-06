/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb.cache.linked;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
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
    public void testFailingValueSerializerOnPut() throws IOException, InterruptedException
    {
        try (OHCache<byte[]> cache = OHCacheBuilder.<byte[]>newBuilder()
                                                            .valueSerializer(TestUtils.byteArraySerializerFailSerialize)
                                                            .capacity(512L * 1024 * 1024)
                                                            .fixedValueSize(8)
                                                            .build())
        {
            try
            {
                cache.put(Ints.toByteArray(1), Longs.toByteArray(1));
                Assert.fail();
            }
            catch (RuntimeException ignored)
            {
                // ok
            }

            try
            {
                cache.putIfAbsent(Ints.toByteArray(1), Longs.toByteArray(1));
                Assert.fail();
            }
            catch (RuntimeException ignored)
            {
                // ok
            }

            try
            {
                cache.addOrReplace(Ints.toByteArray(1), Longs.toByteArray(1), Longs.toByteArray(2));
                Assert.fail();
            }
            catch (RuntimeException ignored)
            {
                // ok
            }

        }
    }
}
