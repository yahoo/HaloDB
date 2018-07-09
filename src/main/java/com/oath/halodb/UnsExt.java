/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import sun.misc.Unsafe;

abstract class UnsExt {

    final Unsafe unsafe;

    UnsExt(Unsafe unsafe) {
        this.unsafe = unsafe;
    }

    abstract long getAndPutLong(long address, long offset, long value);

    abstract int getAndAddInt(long address, long offset, int value);

    abstract long crc32(long address, long offset, long len);
}
