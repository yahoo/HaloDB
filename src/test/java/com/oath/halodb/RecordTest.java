/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * @author Arjun Mannaly
 */
public class RecordTest {

    @Test
    public void testSerializeHeader() {

        byte keySize = 8;
        int valueSize = 100;
        long sequenceNumber = 34543434343L;

        Record.Header header = new Record.Header(0, keySize, valueSize, sequenceNumber);
        ByteBuffer serialized = header.serialize();

        Assert.assertEquals(keySize, serialized.get(Record.Header.KEY_SIZE_OFFSET));
        Assert.assertEquals(valueSize, serialized.getInt(Record.Header.VALUE_SIZE_OFFSET));
        Assert.assertEquals(sequenceNumber, serialized.getLong(Record.Header.SEQUENCE_NUMBER_OFFSET));
    }

    @Test
    public void testDeserializeHeader() {

        long checkSum = 23434;
        byte keySize = 8;
        int valueSize = 100;
        long sequenceNumber = 34543434343L;

        ByteBuffer buffer = ByteBuffer.allocate(Record.Header.HEADER_SIZE);
        buffer.putLong(checkSum);
        buffer.put(keySize);
        buffer.putInt(valueSize);
        buffer.putLong(sequenceNumber);
        buffer.flip();

        Record.Header header = Record.Header.deserialize(buffer);

        Assert.assertEquals(checkSum, header.getCheckSum());
        Assert.assertEquals(keySize, header.getKeySize());
        Assert.assertEquals(valueSize, header.getValueSize());
        Assert.assertEquals(sequenceNumber, header.getSequenceNumber());
        Assert.assertEquals(keySize + valueSize + Record.Header.HEADER_SIZE, header.getRecordSize());
    }

    @Test
    public void testSerializeRecord() {
        byte[] key = TestUtils.generateRandomByteArray();
        byte[] value = TestUtils.generateRandomByteArray();
        long sequenceNumber = 192;

        Record record = new Record(key, value);
        record.setSequenceNumber(sequenceNumber);

        ByteBuffer[] buffers = record.serialize();
        CRC32 crc32 = new CRC32();
        crc32.update(buffers[0].array(), Record.Header.KEY_SIZE_OFFSET, buffers[0].array().length - Record.Header.KEY_SIZE_OFFSET);
        crc32.update(key);
        crc32.update(value);

        Record.Header header = new Record.Header(0, (byte)key.length, value.length, sequenceNumber);
        ByteBuffer headerBuf = header.serialize();
        headerBuf.putLong(Record.Header.CHECKSUM_OFFSET, crc32.getValue());

        Assert.assertEquals(headerBuf, buffers[0]);
        Assert.assertEquals(ByteBuffer.wrap(key), buffers[1]);
        Assert.assertEquals(ByteBuffer.wrap(value), buffers[2]);
    }
}
