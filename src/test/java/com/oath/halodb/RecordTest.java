/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import org.testng.Assert;
import org.testng.annotations.Test;

public class RecordTest {

    @Test
    public void testSerializeHeader() {

        int keySize = 300;  // 256 plus 44
        int valueSize = 100;
        long sequenceNumber = 34543434343L;
        byte version = 29;

        RecordEntry.Header header = new RecordEntry.Header(0, version, keySize, valueSize, sequenceNumber);
        ByteBuffer serialized = header.serialize();

        Assert.assertEquals(serialized.get(RecordEntry.Header.KEY_SIZE_OFFSET) & 0xFF, keySize & 0xFF);
        Assert.assertEquals(serialized.getInt(RecordEntry.Header.VALUE_SIZE_OFFSET), valueSize);
        Assert.assertEquals(serialized.getLong(RecordEntry.Header.SEQUENCE_NUMBER_OFFSET), sequenceNumber);
        Assert.assertEquals(serialized.get(RecordEntry.Header.VERSION_OFFSET) & 0xFF, (version << 3) | (keySize >>> 8));
    }

    @Test
    public void testDeserializeHeader() {

        long checkSum = 23434;
        int keySize = 200;
        int valueSize = 100;
        long sequenceNumber = 34543434343L;
        int version = 2;

        ByteBuffer buffer = ByteBuffer.allocate(RecordEntry.Header.HEADER_SIZE);
        buffer.putInt(Utils.toSignedIntFromLong(checkSum));
        buffer.put((byte)(version << 3));
        buffer.put((byte)keySize);
        buffer.putInt(valueSize);
        buffer.putLong(sequenceNumber);
        buffer.flip();

        RecordEntry.Header header = RecordEntry.Header.deserialize(buffer);

        Assert.assertEquals(checkSum, header.getCheckSum());
        Assert.assertEquals(version, header.getVersion());
        Assert.assertEquals(keySize, header.getKeySize());
        Assert.assertEquals(valueSize, header.getValueSize());
        Assert.assertEquals(sequenceNumber, header.getSequenceNumber());
        Assert.assertEquals(keySize + valueSize + RecordEntry.Header.HEADER_SIZE, header.getRecordSize());
    }

    @Test
    public void testSerializeRecord() {
        byte[] key = TestUtils.generateRandomByteArray();
        byte[] value = TestUtils.generateRandomByteArray();
        long sequenceNumber = 192;
        byte version = 13;

        RecordEntry.Header header = new RecordEntry.Header(0, version, key.length, value.length, sequenceNumber);
        RecordEntry record = new RecordEntry(header, key, value);

        ByteBuffer[] buffers = record.serialize();
        CRC32 crc32 = new CRC32();
        crc32.update(buffers[0].array(), RecordEntry.Header.VERSION_OFFSET, buffers[0].array().length - RecordEntry.Header.CHECKSUM_SIZE);
        crc32.update(key);
        crc32.update(value);

        ByteBuffer headerBuf = header.serialize();
        headerBuf.putInt(RecordEntry.Header.CHECKSUM_OFFSET, Utils.toSignedIntFromLong(crc32.getValue()));

        Assert.assertEquals(headerBuf, buffers[0]);
        Assert.assertEquals(ByteBuffer.wrap(key), buffers[1]);
        Assert.assertEquals(ByteBuffer.wrap(value), buffers[2]);
    }
}
