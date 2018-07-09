/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import com.sun.jna.Native;

final class JNANativeAllocator implements NativeMemoryAllocator {

    public long allocate(long size) {
        try {
            return Native.malloc(size);
        } catch (OutOfMemoryError oom) {
            return 0L;
        }
    }

    public void free(long peer) {
        Native.free(peer);
    }

    public long getTotalAllocated() {
        return -1L;
    }
}
