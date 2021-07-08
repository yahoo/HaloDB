/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import java.util.Arrays;

final class KeyBuffer {

    final byte[] buffer;
    private long hash;

    KeyBuffer(byte[] buffer) {
        this.buffer = buffer;
    }

    long hash() {
        return hash;
    }

    KeyBuffer finish(Hasher hasher) {
        hash = hasher.hash(buffer);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        KeyBuffer keyBuffer = (KeyBuffer) o;

        return buffer.length == keyBuffer.buffer.length && Arrays.equals(keyBuffer.buffer, buffer);
    }

    public int size() {
        return buffer.length;
    }

    @Override
    public int hashCode() {
        return (int) hash;
    }

    private static String pad(int val) {
        String str = Integer.toHexString(val & 0xff);
        while (str.length() == 1) {
            str = '0' + str;
        }
        return str;
    }

    @Override
    public String toString() {
        byte[] b = buffer;
        StringBuilder sb = new StringBuilder(b.length * 3);
        for (int ii = 0; ii < b.length; ii++) {
            if (ii % 8 == 0 && ii != 0) {
                sb.append('\n');
            }
            sb.append(pad(b[ii]));
            sb.append(' ');
        }
        return sb.toString();
    }
}
