/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

public class HashTableValueSerializerTest
{
    @AfterMethod(alwaysRun = true)
    public void deinit()
    {
        Uns.clearUnsDebugForTest();
    }

    @Test
    public void testFailingValueSerializerOnPut() throws IOException, InterruptedException
    {
        ByteArrayEntrySerializer serializer = ByteArrayEntrySerializer.ofSizeFailSerialize(8);
        try (OffHeapHashTable<ByteArrayEntry> cache = OffHeapHashTableBuilder.newBuilder(serializer).build())
        {
            byte[] key = Ints.toByteArray(1);
            ByteArrayEntry entry = serializer.createEntry(key.length, Longs.toByteArray(1));
            try
            {
                cache.put(key, entry);
                Assert.fail();
            }
            catch (RuntimeException ignored)
            {
                // ok
            }

            try
            {
                cache.putIfAbsent(key, entry);
                Assert.fail();
            }
            catch (RuntimeException ignored)
            {
                // ok
            }

            try
            {
                cache.addOrReplace(key, entry, entry);
                Assert.fail();
            }
            catch (RuntimeException ignored)
            {
                // ok
            }

        }
    }
}
