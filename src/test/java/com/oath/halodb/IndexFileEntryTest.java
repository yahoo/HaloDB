/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

/**
 * @author Arjun Mannaly
 */
public class IndexFileEntryTest {

    @Test
    public void serializeIndexFileEntry() {
        byte[] key = TestUtils.generateRandomByteArray(8);
        int recordSize = 1024;
        int recordOffset = 10240;
        byte keySize = (byte) key.length;
        long sequenceNumber = 100;

        IndexFileEntry entry = new IndexFileEntry(key, recordSize, recordOffset, sequenceNumber);
        ByteBuffer[] buffers = entry.serialize();

        ByteBuffer header = ByteBuffer.allocate(IndexFileEntry.INDEX_FILE_HEADER_SIZE);
        header.put(keySize);
        header.putInt(recordSize);
        header.putInt(recordOffset);
        header.putLong(sequenceNumber);
        header.flip();

        Assert.assertEquals(header, buffers[0]);
        Assert.assertEquals(ByteBuffer.wrap(key), buffers[1]);
    }
}
