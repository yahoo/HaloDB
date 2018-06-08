/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb.cache.linked;

import java.util.Arrays;

final class HeapKeyBuffer
{
    private final byte[] array;
    private long hash;

    HeapKeyBuffer(byte[] bytes)
    {
        array = bytes;
    }

    byte[] array()
    {
        return array;
    }

    int size()
    {
        return array.length;
    }

    long hash()
    {
        return hash;
    }

    HeapKeyBuffer finish(Hasher hasher)
    {
        hash = hasher.hash(array);

        return this;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HeapKeyBuffer keyBuffer = (HeapKeyBuffer) o;

        return Arrays.equals(array, keyBuffer.array);
    }

    public int hashCode()
    {
        return (int) hash;
    }

    static String padToEight(int val)
    {
        String str = Integer.toBinaryString(val & 0xff);
        while (str.length() < 8)
            str = '0' + str;
        return str;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        for (int ii = 0; ii < array.length; ii++) {
            if (ii % 8 == 0 && ii != 0) sb.append('\n');
            sb.append(padToEight(array[ii]));
            sb.append(' ');
        }
        return sb.toString();
    }
}
