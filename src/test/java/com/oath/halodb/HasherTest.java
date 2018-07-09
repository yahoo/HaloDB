/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Random;

public class HasherTest
{
    @Test
    public void testMurmur3()
    {
        test(HashAlgorithm.MURMUR3);
    }

    @Test
    public void testCRC32()
    {
        test(HashAlgorithm.CRC32);
    }

    @Test
    public void testXX()
    {
        test(HashAlgorithm.XX);
    }

    private void test(HashAlgorithm hash)
    {
        Random rand = new Random();

        byte[] buf = new byte[3211];
        rand.nextBytes(buf);

        Hasher hasher = Hasher.create(hash);
        long arrayVal = hasher.hash(buf);
        long memAddr = Uns.allocate(buf.length + 99);
        try
        {
            Uns.copyMemory(buf, 0, memAddr, 99L, buf.length);

            long memoryVal = hasher.hash(memAddr, 99L, buf.length);

            Assert.assertEquals(memoryVal, arrayVal);
        }
        finally
        {
            Uns.free(memAddr);
        }
    }
}
