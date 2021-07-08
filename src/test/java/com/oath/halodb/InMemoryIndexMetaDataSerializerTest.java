/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import org.testng.Assert;
import org.testng.annotations.Test;

public class InMemoryIndexMetaDataSerializerTest implements HashEntrySerializerTest {

    InMemoryIndexMetaDataSerializer serializer = new InMemoryIndexMetaDataSerializer();
    InMemoryIndexMetaData data1 = new InMemoryIndexMetaData(1000, 2000, 3000, 4000, 1234);
    InMemoryIndexMetaData data2 = new InMemoryIndexMetaData(1, 2, 3, 4, 5);

    boolean equalData(InMemoryIndexMetaData e1, InMemoryIndexMetaData e2) {
        return e1.getFileId() == e2.getFileId()
            && e1.getKeySize() == e2.getKeySize()
            && e1.getSequenceNumber() == e2.getSequenceNumber()
            && e1.getValueOffset() == e2.getValueOffset()
            && e1.getValueSize() == e2.getValueSize();
    }

    boolean nothingEqual(InMemoryIndexMetaData e1, InMemoryIndexMetaData e2) {
        return e1.getFileId() != e2.getFileId()
            && e1.getKeySize() != e2.getKeySize()
            && e1.getSequenceNumber() != e2.getSequenceNumber()
            && e1.getValueOffset() != e2.getValueOffset()
            && e1.getValueSize() != e2.getValueSize();
    }

    @Test
    public void testSerializeDeserialize() {

        InMemoryIndexMetaData data1read = testSerDe(data1, serializer, this::equalData);
        InMemoryIndexMetaData data2read = testSerDe(data2, serializer, this::equalData);

        Assert.assertTrue(nothingEqual(data1, data2));
        Assert.assertTrue(nothingEqual(data1read, data2read));
    }

    @Test
    public void testFromRecordHeader() {
        RecordEntry.Header header = new RecordEntry.Header(1, (byte)2, 1000, 2000, 9999);
        InMemoryIndexMetaData data = new InMemoryIndexMetaData(header, 33, 55);
        Assert.assertEquals(data.getFileId(), 33);
        Assert.assertEquals(data.getKeySize(), header.getKeySize());
        Assert.assertEquals(data.getValueOffset(), RecordEntry.getValueOffset(55, data.getKeySize()));
        Assert.assertEquals(data.getValueSize(), header.getValueSize());
        Assert.assertEquals(data.getSequenceNumber(), header.getSequenceNumber());
    }

    @Test
    public void testFromIndexEntry() {
        IndexFileEntry entry = new IndexFileEntry(new byte[33], 88, 101, 5555, (byte)2, 2342323422L);
        InMemoryIndexMetaData data = new InMemoryIndexMetaData(entry, 33);
        Assert.assertEquals(data.getFileId(), 33);
        Assert.assertEquals(data.getKeySize(), entry.getKey().length);
        Assert.assertEquals(data.getValueOffset(), RecordEntry.getValueOffset(entry.getRecordOffset(), data.getKeySize()));
        Assert.assertEquals(data.getValueSize(), RecordEntry.getValueSize(entry.getRecordSize(), data.getKeySize()));
        Assert.assertEquals(data.getSequenceNumber(), entry.getSequenceNumber());
    }

    @Test
    public void testRelocate() {
        Assert.assertEquals(data1.getFileId(), 1000);
        Assert.assertEquals(data1.getValueOffset(), 2000);
        InMemoryIndexMetaData relocated = data1.relocated(77, 1234);
        Assert.assertEquals(relocated.getFileId(), 77);
        Assert.assertEquals(relocated.getKeySize(), data1.getKeySize());
        Assert.assertEquals(relocated.getValueOffset(), RecordEntry.getValueOffset(1234, data1.getKeySize()));
        Assert.assertEquals(relocated.getValueSize(), data1.getValueSize());
        Assert.assertEquals(relocated.getSequenceNumber(), data1.getSequenceNumber());
    }
}
