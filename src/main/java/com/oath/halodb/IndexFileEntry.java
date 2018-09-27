/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * This is what is stored in the index file.
 *
 * @author Arjun Mannaly
 */
class IndexFileEntry {

    /**
     * checksum         - 4 bytes. 
     * version          - 1 byte.
     * Key size         - 1 bytes.
     * record size      - 4 bytes.
     * record offset    - 4 bytes.
     * sequence number  - 8 bytes
     */
    final static int INDEX_FILE_HEADER_SIZE = 22;
    final static int CHECKSUM_SIZE = 4;

    static final int CHECKSUM_OFFSET = 0;
    static final int VERSION_OFFSET = 4;
    static final int KEY_SIZE_OFFSET = 5;
    static final int RECORD_SIZE_OFFSET = 6;
    static final int RECORD_OFFSET = 10;
    static final int SEQUENCE_NUMBER_OFFSET = 14;


    private final byte[] key;
    private final int recordSize;
    private final int recordOffset;
    private final byte keySize;
    private final int version;
    private final long sequenceNumber;
    private final long checkSum;

    IndexFileEntry(byte[] key, int recordSize, int recordOffset, long sequenceNumber, int version, long checkSum) {
        this.key = key;
        this.recordSize = recordSize;
        this.recordOffset = recordOffset;
        this.sequenceNumber = sequenceNumber;
        this.version = version;
        this.checkSum = checkSum;

        this.keySize = (byte)key.length;
    }

    ByteBuffer[] serialize() {
        byte[] header = new byte[INDEX_FILE_HEADER_SIZE];
        ByteBuffer h = ByteBuffer.wrap(header);

        h.put(VERSION_OFFSET, (byte)version);
        h.put(KEY_SIZE_OFFSET, keySize);
        h.putInt(RECORD_SIZE_OFFSET, recordSize);
        h.putInt(RECORD_OFFSET, recordOffset);
        h.putLong(SEQUENCE_NUMBER_OFFSET, sequenceNumber);
        long crc32 = computeCheckSum(h.array());
        h.putInt(CHECKSUM_OFFSET, Utils.toSignedIntFromLong(crc32));

        return new ByteBuffer[] { h, ByteBuffer.wrap(key) };
    }

    static IndexFileEntry deserialize(ByteBuffer buffer) {
        long crc32 = Utils.toUnsignedIntFromInt(buffer.getInt());
        int version = Utils.toUnsignedByte(buffer.get());
        byte keySize = buffer.get();
        int recordSize = buffer.getInt();
        int offset = buffer.getInt();
        long sequenceNumber = buffer.getLong();

        byte[] key = new byte[keySize];
        buffer.get(key);

        return new IndexFileEntry(key, recordSize, offset, sequenceNumber, version, crc32);
    }

    static IndexFileEntry deserializeIfNotCorrupted(ByteBuffer buffer) {
        if (buffer.remaining() < INDEX_FILE_HEADER_SIZE) {
            return null;
        }

        long crc32 = Utils.toUnsignedIntFromInt(buffer.getInt());
        int version = Utils.toUnsignedByte(buffer.get());
        byte keySize = buffer.get();
        int recordSize = buffer.getInt();
        int offset = buffer.getInt();
        long sequenceNumber = buffer.getLong();
        if (sequenceNumber < 0 || keySize <= 0
            || version < 0 || version > 255
            || recordSize <= 0 || offset < 0
            || buffer.remaining() < keySize) {
            return null;
        }

        byte[] key = new byte[keySize];
        buffer.get(key);

        IndexFileEntry entry = new IndexFileEntry(key, recordSize, offset, sequenceNumber, version, crc32);
        if (entry.computeCheckSum() != entry.checkSum) {
            return null;
        }

        return entry;
    }

    private long computeCheckSum(byte[] header) {
        CRC32 crc32 = new CRC32();
        crc32.update(header, CHECKSUM_OFFSET + CHECKSUM_SIZE, INDEX_FILE_HEADER_SIZE - CHECKSUM_SIZE);
        crc32.update(key);
        return crc32.getValue();
    }

    long computeCheckSum() {
        ByteBuffer header = ByteBuffer.allocate(INDEX_FILE_HEADER_SIZE);
        header.put(VERSION_OFFSET, (byte)version);
        header.put(KEY_SIZE_OFFSET, keySize);
        header.putInt(RECORD_SIZE_OFFSET, recordSize);
        header.putInt(RECORD_OFFSET, recordOffset);
        header.putLong(SEQUENCE_NUMBER_OFFSET, sequenceNumber);
        return computeCheckSum(header.array());
    }

    byte[] getKey() {
        return key;
    }

    int getRecordSize() {
        return recordSize;
    }

    int getRecordOffset() {
        return recordOffset;
    }

    long getSequenceNumber() {
        return sequenceNumber;
    }

    int getVersion() {
        return version;
    }

    long getCheckSum() {
        return checkSum;
    }
}
