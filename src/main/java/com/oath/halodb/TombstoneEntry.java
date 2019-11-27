/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

class TombstoneEntry {
    /**
     * crc                - 4 bytes.
     * version + key size - 2 bytes.  5 bits for version, 11 for keySize,
     * Sequence number    - 8 bytes.
     */
    static final int TOMBSTONE_ENTRY_HEADER_SIZE = 4 + 1 + 1 + 8;
    static final int CHECKSUM_SIZE = 4;

    static final int CHECKSUM_OFFSET = 0;
    static final int VERSION_OFFSET = 4;
    static final int SEQUENCE_NUMBER_OFFSET = 5;
    static final int KEY_SIZE_OFFSET = 13;

    private final byte[] key;
    private final long sequenceNumber;
    private final long checkSum;
    private final byte version;

    TombstoneEntry(byte[] key, long sequenceNumber, long checkSum, byte version) {
        this.key = key;
        this.sequenceNumber = sequenceNumber;
        this.checkSum = checkSum;
        this.version = version;
    }

    byte[] getKey() {
        return key;
    }

    long getSequenceNumber() {
        return sequenceNumber;
    }

    byte getVersion() {
        return version;
    }

    long getCheckSum() {
        return checkSum;
    }

    int size() {
        return TOMBSTONE_ENTRY_HEADER_SIZE + key.length;
    }

    private ByteBuffer serializeHeader() {
        ByteBuffer header = ByteBuffer.allocate(TOMBSTONE_ENTRY_HEADER_SIZE);
        header.put(VERSION_OFFSET, Utils.versionByte(version, key.length));
        header.putLong(SEQUENCE_NUMBER_OFFSET, sequenceNumber);
        header.put(KEY_SIZE_OFFSET, Utils.keySizeByte(key.length));
        return header;
    }

    ByteBuffer[] serialize() {
        ByteBuffer header = serializeHeader();
        long crc32 = computeCheckSum(header.array());
        header.putInt(CHECKSUM_OFFSET, Utils.toSignedIntFromLong(crc32));
        return new ByteBuffer[] {header, ByteBuffer.wrap(key)};
    }

    static TombstoneEntry deserialize(ByteBuffer buffer) {
        long crc32 = Utils.toUnsignedIntFromInt(buffer.getInt());
        byte vByte = buffer.get();
        long sequenceNumber = buffer.getLong();
        byte keySizeByte = buffer.get();
        byte version = Utils.version(vByte);
        short keySize = Utils.keySize(vByte, keySizeByte);
        byte[] key = new byte[keySize];
        buffer.get(key);

        return new TombstoneEntry(key, sequenceNumber, crc32, version);
    }

    // returns null if a corrupted entry is detected.
    static TombstoneEntry deserializeIfNotCorrupted(ByteBuffer buffer) {
        if (buffer.remaining() < TOMBSTONE_ENTRY_HEADER_SIZE) {
            return null;
        }
        long crc32 = Utils.toUnsignedIntFromInt(buffer.getInt());
        byte vByte = buffer.get();
        long sequenceNumber = buffer.getLong();
        byte keySizeByte = buffer.get();
        byte version = Utils.version(vByte);
        short keySize = Utils.keySize(vByte, keySizeByte);
        if (sequenceNumber < 0 || keySize < 0 || version < 0 || version > 31 || buffer.remaining() < keySize)
            return null;

        byte[] key = new byte[keySize];
        buffer.get(key);

        TombstoneEntry entry = new TombstoneEntry(key, sequenceNumber, crc32, version);
        if (entry.computeCheckSum() != entry.checkSum) {
            return null;
        }

        return entry;
    }

    private long computeCheckSum(byte[] header) {
        CRC32 crc32 = new CRC32();
        crc32.update(header, CHECKSUM_OFFSET + CHECKSUM_SIZE, TOMBSTONE_ENTRY_HEADER_SIZE - CHECKSUM_SIZE);
        crc32.update(key);
        return crc32.getValue();
    }

    long computeCheckSum() {
        ByteBuffer header = serializeHeader();
        return computeCheckSum(header.array());
    }
}
