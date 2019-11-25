/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32;

public class Record {

    private final byte[] key, value;

    private InMemoryIndexMetaData recordMetaData;

    private Header header;

    public Record(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
        header = new Header(0, Versions.CURRENT_DATA_FILE_VERSION, Utils.validateKeySize(key.length), value.length, -1);
    }

    ByteBuffer[] serialize() {
        ByteBuffer headerBuf = serializeHeaderAndComputeChecksum();
        return new ByteBuffer[] {headerBuf, ByteBuffer.wrap(key), ByteBuffer.wrap(value)};
    }

    static Record deserialize(ByteBuffer buffer, short keySize, int valueSize) {
        buffer.flip();
        byte[] key = new byte[keySize];
        byte[] value = new byte[valueSize];
        buffer.get(key);
        buffer.get(value);
        return new Record(key, value);
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    InMemoryIndexMetaData getRecordMetaData() {
        return recordMetaData;
    }

    void setRecordMetaData(InMemoryIndexMetaData recordMetaData) {
      this.recordMetaData = recordMetaData;
    }

    void setRecordMetaData(int fileId, int offset) {
        this.recordMetaData = new InMemoryIndexMetaData(header, fileId, offset);
    }

    /**
     * @return recordSize which is HEADER_SIZE + key size + value size.
     */
    int getRecordSize() {
        return header.getRecordSize();
    }

    void setSequenceNumber(long sequenceNumber) {
        header.sequenceNumber = sequenceNumber;
    }

    long getSequenceNumber() {
        return header.getSequenceNumber();
    }

    void setVersion(byte version) {
        Utils.validateVersion(version);
        header.version = version;
    }

    int getVersion() {
        return header.version;
    }

    Header getHeader() {
        return header;
    }

    void setHeader(Header header) {
        this.header = header;
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

    private long computeCheckSum(byte[] header) {
        CRC32 crc32 = new CRC32();

        // compute checksum with all but the first header element, key and value.
        crc32.update(header, Header.CHECKSUM_OFFSET + Header.CHECKSUM_SIZE, Header.HEADER_SIZE-Header.CHECKSUM_SIZE);
        crc32.update(key);
        crc32.update(value);
        return crc32.getValue();
    }

    @Override
    public boolean equals(Object obj) {
        // to be used in tests as we don't check if the headers are the same.

        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Record)) {
            return false;
        }

        Record record = (Record)obj;
        return Arrays.equals(getKey(), record.getKey()) && Arrays.equals(getValue(), record.getValue());
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
        private long sequenceNumber;
        private final int recordSize;
        private byte version;
        private final short keySize;

        Header(long checkSum, byte version, int keySize, int valueSize, long sequenceNumber) {
            this.checkSum = checkSum;
            this.version = version;
            this.keySize = Utils.validateKeySize(keySize);
            this.valueSize = valueSize;
            this.sequenceNumber = sequenceNumber;
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

        static boolean verifyHeader(Record.Header header) {
            return header.version >= 0 && header.version < 256
                   &&  header.keySize > 0 && header.valueSize > 0
                   && header.recordSize > 0 && header.sequenceNumber > 0;
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
