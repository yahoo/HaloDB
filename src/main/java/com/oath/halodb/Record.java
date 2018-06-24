/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32;

/**
 * @author Arjun Mannaly
 */
public class Record {

    private final byte[] key, value;

    private RecordMetaDataForCache recordMetaData;

    private Header header;

    public Record(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
        header = new Header(0, (byte)key.length, value.length, -1);
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

    RecordMetaDataForCache getRecordMetaData() {
        return recordMetaData;
    }

    void setRecordMetaData(RecordMetaDataForCache recordMetaData) {
        this.recordMetaData = recordMetaData;
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
        crc32.update(header, Header.KEY_SIZE_OFFSET, Header.HEADER_SIZE-Header.KEY_SIZE_OFFSET);
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
         * crc              - 4 bytes.
         * key size         - 1 bytes.
         * value size       - 4 bytes.
         * sequence number  - 8 bytes.
         */
        static final int CHECKSUM_OFFSET = 0;
        static final int KEY_SIZE_OFFSET = 4;
        static final int VALUE_SIZE_OFFSET = 5;
        static final int SEQUENCE_NUMBER_OFFSET = 9;

        static final int HEADER_SIZE = 17;

        private long checkSum;
        private byte keySize;
        private int valueSize;
        private long sequenceNumber;

        private int recordSize;

        Header(long checkSum, byte keySize, int valueSize, long sequenceNumber) {
            this.checkSum = checkSum;
            this.keySize = keySize;
            this.valueSize = valueSize;
            this.sequenceNumber = sequenceNumber;
            recordSize = keySize + valueSize + HEADER_SIZE;
        }

        static Header deserialize(ByteBuffer buffer) {

            long checkSum = Utils.toUnsignedIntFromInt(buffer.getInt(CHECKSUM_OFFSET));
            byte keySize = buffer.get(KEY_SIZE_OFFSET);
            int valueSize = buffer.getInt(VALUE_SIZE_OFFSET);
            long sequenceNumber = buffer.getLong(SEQUENCE_NUMBER_OFFSET);

            return new Header(checkSum, keySize, valueSize, sequenceNumber);
        }

        // checksum value can be computed only with record key and value. 
        ByteBuffer serialize() {
            byte[] header = new byte[HEADER_SIZE];
            ByteBuffer headerBuffer = ByteBuffer.wrap(header);
            headerBuffer.put(KEY_SIZE_OFFSET, keySize);
            headerBuffer.putInt(VALUE_SIZE_OFFSET, valueSize);
            headerBuffer.putLong(SEQUENCE_NUMBER_OFFSET, sequenceNumber);

            return headerBuffer;
        }

        static boolean verifyHeader(Record.Header header) {
            return header.keySize > 0 && header.valueSize > 0 && header.recordSize > 0 && header.sequenceNumber > 0;
        }

        byte getKeySize() {
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
    }
}
