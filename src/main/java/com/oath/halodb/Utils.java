/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

class Utils {
    static long roundUpToPowerOf2(long number) {
        return (number > 1) ? Long.highestOneBit((number - 1) << 1) : 1;
    }

    static int getValueOffset(int recordOffset, byte[] key) {
        return recordOffset + Record.Header.HEADER_SIZE + key.length;
    }

    //TODO: probably belongs to Record.
    static int getRecordSize(int keySize, int valueSize) {
        return keySize + valueSize + Record.Header.HEADER_SIZE;
    }

    static int getValueSize(int recordSize, byte[] key) {
        return recordSize - Record.Header.HEADER_SIZE - key.length;
    }

    static InMemoryIndexMetaData getMetaData(IndexFileEntry entry, int fileId) {
        return new InMemoryIndexMetaData(fileId, Utils.getValueOffset(entry.getRecordOffset(), entry.getKey()), Utils.getValueSize(entry.getRecordSize(), entry.getKey()), entry.getSequenceNumber());
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
    static void validateVersion(byte version) {
        if ((version >>> 5) != 0) {
            throw new IllegalArgumentException("Version must be between 0 and 31, but was: " + version);
        }
    }

    static short validateKeySize(int keySize) {
        if ((keySize >>> 11) != 0) {
            throw new IllegalArgumentException("Key size must be between 0 and 2047, but was: " + keySize);
        }
        return (short) (keySize & 0xFFFF);
    }
}
