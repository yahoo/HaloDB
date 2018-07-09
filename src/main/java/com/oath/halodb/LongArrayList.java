/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import java.util.Arrays;

final class LongArrayList {

    private long[] array;
    private int size;

    public LongArrayList() {
        this(10);
    }

    public LongArrayList(int initialCapacity) {
        array = new long[initialCapacity];
    }

    public long getLong(int i) {
        if (i < 0 || i >= size) {
            throw new ArrayIndexOutOfBoundsException();
        }
        return array[i];
    }

    public void clear() {
        size = 0;
    }

    public int size() {
        return size;
    }

    public void add(long value) {
        if (size == array.length) {
            array = Arrays.copyOf(array, size * 2);
        }
        array[size++] = value;
    }
}
