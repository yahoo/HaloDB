/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import com.oath.halodb.LongArrayList;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class LongArrayListTest
{
    @Test
    public void testLongArrayList()
    {
        LongArrayList l = new LongArrayList();

        assertEquals(l.size(), 0);

        l.add(0);
        assertEquals(l.size(), 1);

        for (int i=1;i<=20;i++)
        {
            l.add(i);
            assertEquals(l.size(), i + 1);
        }

        for (int i=0;i<=20;i++)
        {
            assertEquals(l.getLong(i), i);
        }
    }
}