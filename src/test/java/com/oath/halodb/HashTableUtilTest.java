/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import org.testng.Assert;
import org.testng.annotations.Test;

public class HashTableUtilTest
{
    static final long BIG = 2L << 40;

    @Test
    public void testBitNum() {
        Assert.assertEquals(HashTableUtil.bitNum(0), 0);
        Assert.assertEquals(HashTableUtil.bitNum(1), 1);
        Assert.assertEquals(HashTableUtil.bitNum(2), 2);
        Assert.assertEquals(HashTableUtil.bitNum(3), 2);
        Assert.assertEquals(HashTableUtil.bitNum(4), 3);
        Assert.assertEquals(HashTableUtil.bitNum(7), 3);
        Assert.assertEquals(HashTableUtil.bitNum(8), 4);
        Assert.assertEquals(HashTableUtil.bitNum(9), 4);
        Assert.assertEquals(HashTableUtil.bitNum(16), 5);
        Assert.assertEquals(HashTableUtil.bitNum(31), 5);
        Assert.assertEquals(HashTableUtil.bitNum(32), 6);
        Assert.assertEquals(HashTableUtil.bitNum(33), 6);
        Assert.assertEquals(HashTableUtil.bitNum(64), 7);
        Assert.assertEquals(HashTableUtil.bitNum(127), 7);
        Assert.assertEquals(HashTableUtil.bitNum(128), 8);
        Assert.assertEquals(HashTableUtil.bitNum(129), 8);
        Assert.assertEquals(HashTableUtil.bitNum(256), 9);
        Assert.assertEquals(HashTableUtil.bitNum(1023), 10);
        Assert.assertEquals(HashTableUtil.bitNum(1024), 11);
        Assert.assertEquals(HashTableUtil.bitNum(1025), 11);
        Assert.assertEquals(HashTableUtil.bitNum(65535), 16);
        Assert.assertEquals(HashTableUtil.bitNum(65536), 17);
        Assert.assertEquals(HashTableUtil.bitNum(65537), 17);
    }

    @Test
    public void testRoundUpToPowerOf2() {
      Assert.assertEquals(HashTableUtil.roundUpToPowerOf2(1, 6), 1);
      Assert.assertEquals(HashTableUtil.roundUpToPowerOf2(2, 6), 2);
      Assert.assertEquals(HashTableUtil.roundUpToPowerOf2(3, 6), 4);
      Assert.assertEquals(HashTableUtil.roundUpToPowerOf2(4, 6), 4);
      Assert.assertEquals(HashTableUtil.roundUpToPowerOf2(5, 6), 8);
      Assert.assertEquals(HashTableUtil.roundUpToPowerOf2(8, 6), 8);
      Assert.assertEquals(HashTableUtil.roundUpToPowerOf2(15, 6), 16);
      Assert.assertEquals(HashTableUtil.roundUpToPowerOf2(16, 6), 16);
      Assert.assertEquals(HashTableUtil.roundUpToPowerOf2(17, 6), 32);
      Assert.assertEquals(HashTableUtil.roundUpToPowerOf2(32, 6), 32);
      Assert.assertEquals(HashTableUtil.roundUpToPowerOf2(63, 6), 64);
      Assert.assertEquals(HashTableUtil.roundUpToPowerOf2(64, 6), 64);
      Assert.assertEquals(HashTableUtil.roundUpToPowerOf2(65, 6), 64);
    }
}
