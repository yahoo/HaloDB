/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * This is what is stored in the index file.
 */
class IndexFileEntry {

    /**
     * checksum           - 4 bytes.
     * version + key size - 2 bytes.  5 bits for version, 11 for keySize
     * record size        - 4 bytes.
     * record offset      - 4 bytes.
     * sequence number    - 8 bytes
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
    private final long sequenceNumber;
    private final long checkSum;
    private final byte version;

    IndexFileEntry(byte[] key, int recordSize, int recordOffset, long sequenceNumber, byte version, long checkSum) {
        this.key = key;
        this.recordSize = recordSize;
        this.recordOffset = recordOffset;
        this.sequenceNumber = sequenceNumber;
        this.version = version;
        this.checkSum = checkSum;
    }

    private ByteBuffer serializeHeader() {
        ByteBuffer header = ByteBuffer.allocate(INDEX_FILE_HEADER_SIZE);
        header.put(VERSION_OFFSET, Utils.versionByte(version, key.length));
        header.put(KEY_SIZE_OFFSET, Utils.keySizeByte(key.length));
        header.putInt(RECORD_SIZE_OFFSET, recordSize);
        header.putInt(RECORD_OFFSET, recordOffset);
        header.putLong(SEQUENCE_NUMBER_OFFSET, sequenceNumber);
        return header;
    }

    ByteBuffer[] serialize() {
        ByteBuffer header = serializeHeader();
        long crc32 = computeCheckSum(header.array());
        header.putInt(CHECKSUM_OFFSET, Utils.toSignedIntFromLong(crc32));
        return new ByteBuffer[] { header, ByteBuffer.wrap(key) };
    }

    static IndexFileEntry deserialize(ByteBuffer buffer) {
        long crc32 = Utils.toUnsignedIntFromInt(buffer.getInt());
        byte vbyte = buffer.get();
        byte keySizeByte = buffer.get();
        int recordSize = buffer.getInt();
        int offset = buffer.getInt();
        byte version = Utils.version(vbyte);
        short keySize = Utils.keySize(vbyte, keySizeByte);
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
        byte vbyte = buffer.get();
        byte keySizeByte = buffer.get();
        int recordSize = buffer.getInt();
        int offset = buffer.getInt();
        byte version = Utils.version(vbyte);
        short keySize = Utils.keySize(vbyte, keySizeByte);
        long sequenceNumber = buffer.getLong();

        if (sequenceNumber < 0 || keySize < 0
            || version < 0 || version > 31
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
        ByteBuffer header = serializeHeader();
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

    byte getVersion() {
        return version;
    }

    long getCheckSum() {
        return checkSum;
    }
}
