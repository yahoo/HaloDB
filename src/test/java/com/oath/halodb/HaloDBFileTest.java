/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.util.List;

public class HaloDBFileTest {

    private File directory = Paths.get("tmp", "HaloDBFileTest",  "testIndexFile").toFile();
    private DBDirectory dbDirectory;
    private HaloDBFile file;
    private IndexFile indexFile;
    private int fileId = 100;
    private File backingFile = directory.toPath().resolve(fileId+HaloDBFile.DATA_FILE_NAME).toFile();
    private FileTime createdTime;

    @BeforeMethod
    public void before() throws IOException {
        TestUtils.deleteDirectory(directory);
        dbDirectory = DBDirectory.open(directory);
        file = HaloDBFile.create(dbDirectory, fileId, new HaloDBOptions(), HaloDBFile.FileType.DATA_FILE);
        createdTime = TestUtils.getFileCreationTime(backingFile);
        indexFile = new IndexFile(fileId, dbDirectory, new HaloDBOptions());
        try {
            // wait for a second to make sure that the file creation time of the repaired file will be different.
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
    }

    @AfterMethod
    public void after() throws IOException {
        if (file != null)
            file.close();
        if (indexFile != null)
            indexFile.close();
        dbDirectory.close();
        TestUtils.deleteDirectory(directory);
    }

    @Test
    public void testIndexFile() throws IOException {
        List<Record> list = insertTestRecords();

        indexFile.open();
        verifyIndexFile(indexFile, list);
    }

    @Test
    public void testFileWithInvalidRecord() throws IOException {
        List<Record> list = insertTestRecords();

        // write a corrupted header to file.
        try(FileChannel channel = FileChannel.open(Paths.get(directory.getCanonicalPath(), fileId + HaloDBFile.DATA_FILE_NAME).toAbsolutePath(), StandardOpenOption.APPEND)) {
            ByteBuffer data = ByteBuffer.wrap("garbage".getBytes());
            channel.write(data);
        }

        HaloDBFile.HaloDBFileIterator iterator = file.newIterator();
        int count = 0;
        while (iterator.hasNext() && count < 100) {
            Record record = iterator.next();
            Assert.assertEquals(record.getKey(), list.get(count++).getKey());
        }

        // 101th record's header is corrupted.
        Assert.assertTrue(iterator.hasNext());
        // Since header is corrupted we won't be able to read it and hence next will return null. 
        Assert.assertNull(iterator.next());
    }

    @Test
    public void testCorruptedHeader() throws IOException {
        List<Record> list = insertTestRecords();

        // write a corrupted header to file.
        // write a corrupted record to file.
        byte[] key = "corrupted key".getBytes();
        byte[] value = "corrupted value".getBytes();
        Record corrupted = new Record(key, value);
        // value length is corrupted. 
        corrupted.setHeader(new Record.Header(0, 0, (byte)key.length, -345445, 1234));
        try(FileChannel channel = FileChannel.open(Paths.get(directory.getCanonicalPath(), fileId + HaloDBFile.DATA_FILE_NAME).toAbsolutePath(), StandardOpenOption.APPEND)) {
            channel.write(corrupted.serialize());
        }

        HaloDBFile.HaloDBFileIterator iterator = file.newIterator();
        int count = 0;
        while (iterator.hasNext() && count < 100) {
            Record r = iterator.next();
            Assert.assertEquals(r.getKey(), list.get(count).getKey());
            Assert.assertEquals(r.getValue(), list.get(count).getValue());
            count++;
        }

        // 101th record's header is corrupted.
        Assert.assertTrue(iterator.hasNext());
        // Since header is corrupted we won't be able to read it and hence next will return null.
        Assert.assertNull(iterator.next());
    }

    @Test
    public void testRebuildIndexFile() throws IOException {
        List<Record> list = insertTestRecords();

        indexFile.delete();

        // make sure that the file is deleted. 
        Assert.assertFalse(Paths.get(directory.getName(), fileId + IndexFile.INDEX_FILE_NAME).toFile().exists());
        file.rebuildIndexFile();
        indexFile.open();
        verifyIndexFile(indexFile, list);
    }

    @Test
    public void testRepairDataFileWithCorruptedValue() throws IOException {
        List<Record> list = insertTestRecords();

        // write a corrupted record to file.
        // the record is corrupted in such a way the the size is unchanged but the contents have changed, thus crc will be different. 
        byte[] key = "corrupted key".getBytes();
        byte[] value = "corrupted value".getBytes();
        Record record = new Record(key, value);
        record.setHeader(new Record.Header(0, 2, (byte)key.length, value.length, 1234));
        try(FileChannel channel = FileChannel.open(Paths.get(directory.getCanonicalPath(), fileId + HaloDBFile.DATA_FILE_NAME).toAbsolutePath(), StandardOpenOption.APPEND)) {
           ByteBuffer[] data = record.serialize();
           data[2] = ByteBuffer.wrap("value corrupted".getBytes());
           channel.write(data);
        }

        HaloDBFile repairedFile = file.repairFile(dbDirectory);
        Assert.assertNotEquals(TestUtils.getFileCreationTime(backingFile), createdTime);
        Assert.assertEquals(repairedFile.getPath(), file.getPath());
        verifyDataFile(list, repairedFile);
        verifyIndexFile(repairedFile.getIndexFile(), list);
    }

    @Test
    public void testRepairDataFileWithInCompleteRecord() throws IOException {
        List<Record> list = insertTestRecords();

        // write a corrupted record to file.
        // value was not completely written to file. 
        byte[] key = "corrupted key".getBytes();
        byte[] value = "corrupted value".getBytes();
        Record record = new Record(key, value);
        record.setHeader(new Record.Header(0, 100, (byte)key.length, value.length, 1234));
        try(FileChannel channel = FileChannel.open(Paths.get(directory.getCanonicalPath(), fileId + HaloDBFile.DATA_FILE_NAME).toAbsolutePath(), StandardOpenOption.APPEND)) {
            ByteBuffer[] data = record.serialize();
            data[2] = ByteBuffer.wrap("missing".getBytes());
            channel.write(data);
        }

        HaloDBFile repairedFile = file.repairFile(dbDirectory);
        Assert.assertNotEquals(TestUtils.getFileCreationTime(backingFile), createdTime);
        Assert.assertEquals(repairedFile.getPath(), file.getPath());
        verifyDataFile(list, repairedFile);
        verifyIndexFile(repairedFile.getIndexFile(), list);
    }

    @Test
    public void testRepairDataFileContainingRecordsWithCorruptedHeader() throws IOException {
        List<Record> list = insertTestRecords();

        // write a corrupted header to file.
        try(FileChannel channel = FileChannel.open(Paths.get(directory.getCanonicalPath(), fileId + HaloDBFile.DATA_FILE_NAME).toAbsolutePath(), StandardOpenOption.APPEND)) {
            ByteBuffer data = ByteBuffer.wrap("garbage".getBytes());
            channel.write(data);
        }

        HaloDBFile repairedFile = file.repairFile(dbDirectory);
        Assert.assertNotEquals(TestUtils.getFileCreationTime(backingFile), createdTime);
        Assert.assertEquals(repairedFile.getPath(), file.getPath());
        verifyDataFile(list, repairedFile);
        verifyIndexFile(repairedFile.getIndexFile(), list);
    }

    @Test
    public void testRepairDataFileContainingRecordsWithValidButCorruptedHeader() throws IOException {
        List<Record> list = insertTestRecords();

        // write a corrupted record to file.
        byte[] key = "corrupted key".getBytes();
        byte[] value = "corrupted value".getBytes();
        Record record = new Record(key, value);
        // header is valid but the value size is incorrect. 
        record.setHeader(new Record.Header(0,101,  (byte)key.length, 5, 1234));
        try(FileChannel channel = FileChannel.open(Paths.get(directory.getCanonicalPath(), fileId + HaloDBFile.DATA_FILE_NAME).toAbsolutePath(), StandardOpenOption.APPEND)) {
            ByteBuffer[] data = record.serialize();
            channel.write(data);
        }

        HaloDBFile repairedFile = file.repairFile(dbDirectory);
        Assert.assertNotEquals(TestUtils.getFileCreationTime(backingFile), createdTime);
        Assert.assertEquals(repairedFile.getPath(), file.getPath());
        verifyDataFile(list, repairedFile);
        verifyIndexFile(repairedFile.getIndexFile(), list);
    }

    private void verifyIndexFile(IndexFile file, List<Record> recordList) throws IOException {
        IndexFile.IndexFileIterator indexFileIterator = file.newIterator();
        int count = 0;
        while (indexFileIterator.hasNext()) {
            IndexFileEntry e = indexFileIterator.next();
            Record r = recordList.get(count++);
            InMemoryIndexMetaData meta = r.getRecordMetaData();
            Assert.assertEquals(e.getKey(), r.getKey());

            int expectedOffset = meta.getValueOffset() - Record.Header.HEADER_SIZE - r.getKey().length;
            Assert.assertEquals(e.getRecordOffset(), expectedOffset);
        }

        Assert.assertEquals(count, recordList.size());
    }

    private List<Record> insertTestRecords() throws IOException {
        List<Record> list = TestUtils.generateRandomData(100);
        for (Record record : list) {
            record.setSequenceNumber(100);
            InMemoryIndexMetaData meta = file.writeRecord(record);
            record.setRecordMetaData(meta);
        }
        return list;
    }

    private void verifyDataFile(List<Record> recordList, HaloDBFile dataFile) throws IOException {
        HaloDBFile.HaloDBFileIterator iterator = dataFile.newIterator();
        int count = 0;
        while (iterator.hasNext()) {
            Record actual = iterator.next();
            Record expected = recordList.get(count++);
            Assert.assertEquals(actual, expected);
        }

        Assert.assertEquals(count, recordList.size());
    }
}
