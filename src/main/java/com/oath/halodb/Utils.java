/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

class Utils {
    static long roundUpToPowerOf2(long number) {
        return (number > 1) ? Long.highestOneBit((number - 1) << 1) : 1;
    }

    static long toUnsignedIntFromInt(int value) {
        return value & 0xffffffffL;
    }

    static int toSignedIntFromLong(long value) {
        return (int)(value & 0xffffffffL);
    }

    static int toUnsignedByte(byte value) {
        return value & 0xFF;
    }

    /* max 31 */
    static byte version(byte versionByte) {
        return (byte) (versionByte >>> 3); // 5 most significant bits
    }

    /* max 2047 */
    static short keySize(byte versionByte, byte keySizeByte) {
        int upper = (versionByte & 0b111) << 8; // lowest three bits of version byte are 3 MSB of keySize
        int lower = 0xFF & keySizeByte;
        return (short) (upper | lower);
    }

    static byte versionByte(byte version, int keySize) {
        validateVersion(version);
        validateKeySize(keySize);
        return (byte)(((version << 3) | (keySize >>> 8)) & 0xFF);
    }

    static byte keySizeByte(int keySize) {
        validateKeySize(keySize);
        return (byte)(keySize & 0xFF);
    }
    static byte validateVersion(byte version) {
        if ((version >>> 5) != 0) {
            throw new IllegalArgumentException("Version must be between 0 and 31, but was: " + version);
        }
        return version;
    }

    static short validateKeySize(int keySize) {
        if ((keySize >>> 11) != 0) {
            throw new IllegalArgumentException("Key size must be between 0 and 2047, but was: " + keySize);
        }
        return (short) (keySize & 0xFFFF);
    }

    static int validateValueSize(int valueSize) {
        if ((valueSize >>> 29) != 0) {
            throw new IllegalArgumentException("Value size must be between 0 and 536870912 (~512MB), but was: " + valueSize);
        }
        return valueSize;
    }

    static long validateSequenceNumber(long sequenceNumber) {
        if (sequenceNumber < 0) {
            throw new IllegalArgumentException("Sequence number must be positive, but was: " + sequenceNumber);
        }
        return sequenceNumber;
    }
}
