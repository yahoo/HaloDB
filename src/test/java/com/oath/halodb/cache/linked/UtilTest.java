/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb.cache.linked;

import org.testng.Assert;
import org.testng.annotations.Test;

public class UtilTest
{
    static final long BIG = 2L << 40;

    @Test
    public void testBitNum()
    {
        Assert.assertEquals(Util.bitNum(0), 0);
        Assert.assertEquals(Util.bitNum(1), 1);
        Assert.assertEquals(Util.bitNum(2), 2);
        Assert.assertEquals(Util.bitNum(4), 3);
        Assert.assertEquals(Util.bitNum(8), 4);
        Assert.assertEquals(Util.bitNum(16), 5);
        Assert.assertEquals(Util.bitNum(32), 6);
        Assert.assertEquals(Util.bitNum(64), 7);
        Assert.assertEquals(Util.bitNum(128), 8);
        Assert.assertEquals(Util.bitNum(256), 9);
        Assert.assertEquals(Util.bitNum(1024), 11);
        Assert.assertEquals(Util.bitNum(65536), 17);
    }
}
