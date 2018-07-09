/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

final class UnsafeAllocator implements NativeMemoryAllocator {

    static final Unsafe unsafe;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public long allocate(long size) {
        try {
            return unsafe.allocateMemory(size);
        } catch (OutOfMemoryError oom) {
            return 0L;
        }
    }

    public void free(long peer) {
        unsafe.freeMemory(peer);
    }

    public long getTotalAllocated() {
        return -1L;
    }
}
