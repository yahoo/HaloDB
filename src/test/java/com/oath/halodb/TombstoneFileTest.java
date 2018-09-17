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
import java.util.ArrayList;
import java.util.List;

/**
 * @author Arjun Mannaly
 */
public class TombstoneFileTest {

    private File directory = new File(TestUtils.getTestDirectory("TombstoneFileTest"));
    private DBDirectory dbDirectory;
    private TombstoneFile file;
    private File backingFile;
    private int fileId = 100;
    private FileTime createdTime;

    @BeforeMethod
    public void before() throws IOException {
        TestUtils.deleteDirectory(directory);
        dbDirectory = DBDirectory.open(directory);
        file = TombstoneFile.create(dbDirectory, fileId, new HaloDBOptions());
        backingFile = directory.toPath().resolve(file.getName()).toFile();
        createdTime = TestUtils.getFileCreationTime(backingFile);
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
        dbDirectory.close();
        TestUtils.deleteDirectory(directory);
    }

    @Test
    public void testRepairFileWithCorruptedEntry() throws IOException {
        int noOfRecords = 1000;
        List<TombstoneEntry> records = insertTestRecords(noOfRecords);

        // add a corrupted entry to the file. 
        int sequenceNumber = noOfRecords + 100;
        TombstoneEntry corrupted = new TombstoneEntry(TestUtils.generateRandomByteArray(), sequenceNumber, -1, 21);
        try(FileChannel channel = FileChannel.open(
            Paths.get(directory.getCanonicalPath(), fileId + TombstoneFile.TOMBSTONE_FILE_NAME).toAbsolutePath(), StandardOpenOption.APPEND)) {
            ByteBuffer[] data = corrupted.serialize();
            ByteBuffer header = data[0];
            // change the sequence number, due to which checksum won't match.
            header.putLong(TombstoneEntry.SEQUENCE_NUMBER_OFFSET, sequenceNumber + 100);
            channel.write(data);
        }

        TombstoneFile repairedFile = file.repairFile(dbDirectory);
        Assert.assertNotEquals(TestUtils.getFileCreationTime(backingFile), createdTime);
        verifyData(repairedFile, records);
    }

    @Test
    public void testRepairFileWithCorruptedKeySize() throws IOException {
        int noOfRecords = 34467;
        List<TombstoneEntry> records = insertTestRecords(noOfRecords);

        // add a corrupted entry to the file.
        int sequenceNumber = noOfRecords + 100;
        TombstoneEntry corrupted = new TombstoneEntry(TestUtils.generateRandomByteArray(), sequenceNumber, -1, 13);
        try(FileChannel channel = FileChannel.open(
            Paths.get(directory.getCanonicalPath(), fileId + TombstoneFile.TOMBSTONE_FILE_NAME).toAbsolutePath(), StandardOpenOption.APPEND)) {
            ByteBuffer[] data = corrupted.serialize();
            ByteBuffer header = data[0];
            // change the sequence number, due to which checksum won't match.
            header.put(TombstoneEntry.KEY_SIZE_OFFSET, (byte)0xFF);
            channel.write(data);
        }

        TombstoneFile repairedFile = file.repairFile(dbDirectory);
        Assert.assertNotEquals(TestUtils.getFileCreationTime(backingFile), createdTime);
        verifyData(repairedFile, records);
    }

    @Test
    public void testRepairFileWithIncompleteEntry() throws IOException {
        int noOfRecords = 14;
        List<TombstoneEntry> records = insertTestRecords(noOfRecords);

        // add a corrupted entry to the file.
        int sequenceNumber = noOfRecords + 100;
        TombstoneEntry corrupted = new TombstoneEntry(TestUtils.generateRandomByteArray(), sequenceNumber, -1, 17);
        try(FileChannel channel = FileChannel.open(
            Paths.get(directory.getCanonicalPath(), fileId + TombstoneFile.TOMBSTONE_FILE_NAME).toAbsolutePath(), StandardOpenOption.APPEND)) {
            ByteBuffer[] data = corrupted.serialize();
            ByteBuffer truncatedKey = ByteBuffer.allocate(corrupted.getKey().length/2);
            truncatedKey.put(TestUtils.generateRandomByteArray(corrupted.getKey().length/2));
            truncatedKey.flip();
            data[1] = truncatedKey;
            channel.write(data);
        }

        TombstoneFile repairedFile = file.repairFile(dbDirectory);
        Assert.assertNotEquals(TestUtils.getFileCreationTime(backingFile), createdTime);
        verifyData(repairedFile, records);
    }

    private void verifyData(TombstoneFile file, List<TombstoneEntry> records) throws IOException {
        TombstoneFile.TombstoneFileIterator iterator = file.newIterator();
        int count = 0;
        while (iterator.hasNext()) {
            TombstoneEntry actual = iterator.next();
            Assert.assertEquals(actual.getKey(), records.get(count).getKey());
            Assert.assertEquals(actual.getSequenceNumber(), records.get(count).getSequenceNumber());
            Assert.assertEquals(actual.getVersion(), records.get(count).getVersion());
            Assert.assertEquals(actual.getCheckSum(), records.get(count).getCheckSum());
            count++;
        }

        Assert.assertEquals(count, records.size());

    }

    private List<TombstoneEntry> insertTestRecords(int number) throws IOException {
        List<TombstoneEntry> records = new ArrayList<>();
        for (int i = 0; i < number; i++) {
            TombstoneEntry e = new TombstoneEntry(TestUtils.generateRandomByteArray(), i, -1, 1);
            file.write(e);
            records.add(new TombstoneEntry(e.getKey(), e.getSequenceNumber(), e.computeCheckSum(), e.getVersion()));
        }
        return records;
    }

}
