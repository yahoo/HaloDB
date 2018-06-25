/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import java.nio.ByteBuffer;

/**
 * This is what is stored in the index file.
 *
 * @author Arjun Mannaly
 */
class IndexFileEntry {

    /**
     * version          - 1 byte. 
     * Key size         - 1 bytes.
     * record size      - 4 bytes.
     * record offset    - 4 bytes.
     * sequence number  - 8 bytes
     */
    final static int INDEX_FILE_HEADER_SIZE = 18;

    static final int VERSION_OFFSET = 0;
    static final int KEY_SIZE_OFFSET = 1;
    static final int RECORD_SIZE_OFFSET = 2;
    static final int RECORD_OFFSET = 6;
    static final int SEQUENCE_NUMBER_OFFSET = 10;


    private final byte[] key;
    private final int recordSize;
    private final int recordOffset;
    private final byte keySize;
    private final int version;
    private final long sequenceNumber;

    IndexFileEntry(byte[] key, int recordSize, int recordOffset, long sequenceNumber, int version) {
        this.key = key;
        this.recordSize = recordSize;
        this.recordOffset = recordOffset;
        this.sequenceNumber = sequenceNumber;
        this.version = version;

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

        return new ByteBuffer[] { h, ByteBuffer.wrap(key) };
    }

    static IndexFileEntry deserialize(ByteBuffer buffer) {
        int version = Utils.toUnsignedByte(buffer.get());
        byte keySize = buffer.get();
        int recordSize = buffer.getInt();
        int offset = buffer.getInt();
        long sequenceNumber = buffer.getLong();

        byte[] key = new byte[keySize];
        buffer.get(key);

        return new IndexFileEntry(key, recordSize, offset, sequenceNumber, version);
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
}
