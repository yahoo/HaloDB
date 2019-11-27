/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/** The internal record variant that represents an entry in the record file **/
class RecordEntry extends Record {

    private final Header header;

    public RecordEntry(Header header, byte[] key, byte[] value) {
        super(key, value);
        this.header = header;
        if (key.length != header.keySize) {
            throw new IllegalArgumentException("Key size does not match header data. header: " +
                header.keySize + " actual: " + key.length);
        }
        if (value.length != header.valueSize) {
            throw new IllegalArgumentException("Value size does not match header data. header: " +
                header.valueSize + " actual: " + value.length);
        }
    }

    /** create a RecordEntry from a Record, with a Header that has not yet computed its checksum **/
    static RecordEntry newEntry(Record record, long sequenceNumber) {
        return newEntry(record.getKey(), record.getValue(), sequenceNumber);
    }

    /** create a RecordEntry from a Record, with a Header that has not yet computed its checksum **/
    static RecordEntry newEntry(byte[] key, byte[] value, long sequenceNumber) {
        Header header = new Header(0, Versions.CURRENT_DATA_FILE_VERSION, key.length, value.length, sequenceNumber);
        return new RecordEntry(header, key, value);
    }

    ByteBuffer[] serialize() {
        ByteBuffer headerBuf = serializeHeaderAndComputeChecksum();
        return new ByteBuffer[] {headerBuf, ByteBuffer.wrap(getKey()), ByteBuffer.wrap(getValue())};
    }

    static RecordEntry deserialize(Header header, ByteBuffer buffer) {
        buffer.flip();
        byte[] key = new byte[header.keySize];
        byte[] value = new byte[header.valueSize];
        buffer.get(key);
        buffer.get(value);
        return new RecordEntry(header, key, value);
    }

    static int getValueOffset(int recordOffset, int keySize) {
        return recordOffset + Header.HEADER_SIZE + keySize;
    }

    static int getRecordSize(int keySize, int valueSize) {
        return keySize + valueSize + Header.HEADER_SIZE;
    }

    static int getValueSize(int recordSize, int keySize) {
        return recordSize - Header.HEADER_SIZE - keySize;
    }

    /**
     * @return recordSize which is HEADER_SIZE + key size + value size.
     */
    int getRecordSize() {
        return header.getRecordSize();
    }

    long getSequenceNumber() {
        return header.getSequenceNumber();
    }

    int getVersion() {
        return header.version;
    }

    Header getHeader() {
        return header;
    }

    private ByteBuffer serializeHeaderAndComputeChecksum() {
        ByteBuffer headerBuf = header.serialize();
        long checkSum = computeCheckSum(headerBuf.array());
        headerBuf.putInt(Header.CHECKSUM_OFFSET, Utils.toSignedIntFromLong(checkSum));
        return headerBuf;
    }

    boolean verifyChecksum() {
        ByteBuffer headerBuf = header.serialize();
        long checkSum = computeCheckSum(headerBuf.array());
        return checkSum == header.getCheckSum();
    }

    long computeCheckSum(byte[] header) {
        CRC32 crc32 = new CRC32();

        // compute checksum with all but the first header element, key and value.
        crc32.update(header, Header.CHECKSUM_OFFSET + Header.CHECKSUM_SIZE, Header.HEADER_SIZE-Header.CHECKSUM_SIZE);
        crc32.update(getKey());
        crc32.update(getValue());
        return crc32.getValue();
    }

    static class Header {
        /**
         * crc                - 4 bytes.
         * version + key size - 2 bytes.  5 bits for version, 11 for keySize
         * value size         - 4 bytes.
         * sequence number    - 8 bytes.
         */
        static final int CHECKSUM_OFFSET = 0;
        static final int VERSION_OFFSET = 4;
        static final int KEY_SIZE_OFFSET = 5;
        static final int VALUE_SIZE_OFFSET = 6;
        static final int SEQUENCE_NUMBER_OFFSET = 10;

        static final int HEADER_SIZE = 18;
        static final int CHECKSUM_SIZE = 4;

        private final long checkSum;
        private final int valueSize;
        private final long sequenceNumber;
        private final int recordSize;
        private final byte version;
        private final short keySize;

        Header(long checkSum, byte version, int keySize, int valueSize, long sequenceNumber) {
            this.checkSum = checkSum;
            this.version = Utils.validateVersion(version);
            this.keySize = Utils.validateKeySize(keySize);
            this.valueSize = Utils.validateValueSize(valueSize);
            this.sequenceNumber = Utils.validateSequenceNumber(sequenceNumber);
            this.recordSize = keySize + valueSize + HEADER_SIZE;
        }

        static Header deserialize(ByteBuffer buffer) {

            long checkSum = Utils.toUnsignedIntFromInt(buffer.getInt(CHECKSUM_OFFSET));
            byte vByte = buffer.get(VERSION_OFFSET);
            byte keySizeByte = buffer.get(KEY_SIZE_OFFSET);
            int valueSize = buffer.getInt(VALUE_SIZE_OFFSET);
            long sequenceNumber = buffer.getLong(SEQUENCE_NUMBER_OFFSET);
            byte version = Utils.version(vByte);
            short keySize = Utils.keySize(vByte, keySizeByte);

            return new Header(checkSum, version, keySize, valueSize, sequenceNumber);
        }

        // checksum value can be computed only with record key and value.
        ByteBuffer serialize() {
            byte[] header = new byte[HEADER_SIZE];
            ByteBuffer headerBuffer = ByteBuffer.wrap(header);
            headerBuffer.put(VERSION_OFFSET, Utils.versionByte(version, keySize));
            headerBuffer.put(KEY_SIZE_OFFSET, Utils.keySizeByte(keySize));
            headerBuffer.putInt(VALUE_SIZE_OFFSET, valueSize);
            headerBuffer.putLong(SEQUENCE_NUMBER_OFFSET, sequenceNumber);
            return headerBuffer;
        }

        short getKeySize() {
            return keySize;
        }

        int getValueSize() {
            return valueSize;
        }

        int getRecordSize() {
            return recordSize;
        }

        long getSequenceNumber() {
            return sequenceNumber;
        }

        long getCheckSum() {
            return checkSum;
        }

        short getVersion() {
            return version;
        }
    }
}
