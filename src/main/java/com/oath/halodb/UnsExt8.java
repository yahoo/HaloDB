/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import sun.misc.Unsafe;

import java.util.zip.CRC32;

final class UnsExt8 extends UnsExt {

    UnsExt8(Unsafe unsafe) {
        super(unsafe);
    }

    long getAndPutLong(long address, long offset, long value) {
        return unsafe.getAndSetLong(null, address + offset, value);
    }

    int getAndAddInt(long address, long offset, int value) {
        return unsafe.getAndAddInt(null, address + offset, value);
    }

    long crc32(long address, long offset, long len) {
        CRC32 crc = new CRC32();
        crc.update(Uns.directBufferFor(address, offset, len, true));
        long h = crc.getValue();
        h |= h << 32;
        return h;
    }
}
