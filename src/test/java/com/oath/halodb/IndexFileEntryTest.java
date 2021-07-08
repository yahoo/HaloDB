/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import static com.oath.halodb.IndexFileEntry.CHECKSUM_OFFSET;
import static com.oath.halodb.IndexFileEntry.CHECKSUM_SIZE;
import static com.oath.halodb.IndexFileEntry.INDEX_FILE_HEADER_SIZE;
import static com.oath.halodb.IndexFileEntry.KEY_SIZE_OFFSET;
import static com.oath.halodb.IndexFileEntry.RECORD_OFFSET;
import static com.oath.halodb.IndexFileEntry.RECORD_SIZE_OFFSET;
import static com.oath.halodb.IndexFileEntry.SEQUENCE_NUMBER_OFFSET;
import static com.oath.halodb.IndexFileEntry.VERSION_OFFSET;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import org.testng.Assert;
import org.testng.annotations.Test;

public class IndexFileEntryTest {

    @Test
    public void serializeIndexFileEntry() {
        byte[] key = TestUtils.generateRandomByteArray(1234);
        int recordSize = 1024;
        int recordOffset = 10240;
        int keySize = key.length;
        long sequenceNumber = 100;
        byte version = 20;

        ByteBuffer header = ByteBuffer.allocate(INDEX_FILE_HEADER_SIZE);
        header.put(VERSION_OFFSET, Utils.versionByte(version, keySize));
        header.put(KEY_SIZE_OFFSET, Utils.keySizeByte(keySize));
        header.putInt(RECORD_SIZE_OFFSET, recordSize);
        header.putInt(RECORD_OFFSET, recordOffset);
        header.putLong(SEQUENCE_NUMBER_OFFSET, sequenceNumber);

        CRC32 crc32 = new CRC32();
        crc32.update(header.array(), VERSION_OFFSET, INDEX_FILE_HEADER_SIZE-CHECKSUM_SIZE);
        crc32.update(key);
        long checkSum = crc32.getValue();
        header.putInt(CHECKSUM_OFFSET, Utils.toSignedIntFromLong(checkSum));

        IndexFileEntry entry = new IndexFileEntry(key, recordSize, recordOffset, sequenceNumber, version, -1);
        ByteBuffer[] buffers = entry.serialize();

        Assert.assertEquals(header, buffers[0]);
        Assert.assertEquals(ByteBuffer.wrap(key), buffers[1]);
    }

    @Test
    public void deserializeIndexFileEntry() {
        byte[] key = TestUtils.generateRandomByteArray(300);
        int recordSize = 1024;
        int recordOffset = 10240;
        short keySize = (short) key.length;
        long sequenceNumber = 100;
        int version = 10;
        long checksum = 42323;

        ByteBuffer header = ByteBuffer.allocate(IndexFileEntry.INDEX_FILE_HEADER_SIZE + keySize);
        header.putInt((int)checksum);
        header.put((byte)((version << 3) | (keySize >>> 8)));
        header.put((byte)(keySize & 0xFF));
        header.putInt(recordSize);
        header.putInt(recordOffset);
        header.putLong(sequenceNumber);
        header.put(key);
        header.flip();

        IndexFileEntry entry = IndexFileEntry.deserialize(header);

        Assert.assertEquals(entry.getCheckSum(), checksum);
        Assert.assertEquals(entry.getVersion(), version);
        Assert.assertEquals(entry.getRecordSize(), recordSize);
        Assert.assertEquals(entry.getRecordOffset(), recordOffset);
        Assert.assertEquals(entry.getSequenceNumber(), sequenceNumber);
        Assert.assertEquals(entry.getKey(), key);
    }
}
