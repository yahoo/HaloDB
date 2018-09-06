/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Arjun Mannaly
 */
public class DBRepairTest extends TestBase {

    @Test(dataProvider = "Options")
    public void testRepairDB(HaloDBOptions options) throws HaloDBException, IOException {
        String directory = TestUtils.getTestDirectory("DBRepairTest", "testRepairDB");

        options.setMaxFileSize(1024 * 1024);
        options.setCompactionDisabled(true);

        HaloDB db = getTestDB(directory, options);
        int noOfRecords = 5 * 1024 + 512; // 5 files with 1024 records and 1 with 512 records. 

        List<Record> records = TestUtils.insertRandomRecordsOfSize(db, noOfRecords, 1024-Record.Header.HEADER_SIZE);

        // delete half the records.
        for (int i = 0; i < noOfRecords; i++) {
            if (i % 2 == 0) {
                db.delete(records.get(i).getKey());
            }
        }

        FileTime latestDataFileCreatedTime =
            TestUtils.getFileCreationTime(TestUtils.getLatestDataFile(directory).get());
        FileTime latestTombstoneFileCreationTime =
            TestUtils.getFileCreationTime(FileUtils.listTombstoneFiles(new File(directory))[0]);

        db.close();

        // trick the db to think that there was an unclean shutdown.
        DBMetaData dbMetaData = new DBMetaData(dbDirectory);
        dbMetaData.setOpen(true);
        dbMetaData.storeToFile();

        // wait for a second so that the new file will have a different create at time.
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }

        db = getTestDBWithoutDeletingFiles(directory, options);

        // latest file should have been repaired and replaced.
        Assert.assertNotEquals(
            TestUtils.getFileCreationTime(TestUtils.getLatestDataFile(directory).get()),
            latestDataFileCreatedTime
        );
        Assert.assertNotEquals(
            TestUtils.getFileCreationTime(FileUtils.listTombstoneFiles(new File(directory))[0]),
            latestTombstoneFileCreationTime
        );

        Assert.assertEquals(db.size(), noOfRecords/2);
        for (int i = 0; i < noOfRecords; i++) {
            if (i % 2 == 0) {
                Assert.assertNull(db.get(records.get(i).getKey()));
            }
            else {
                Record r = records.get(i);
                Assert.assertEquals(db.get(r.getKey()), r.getValue());
            }
        }
    }

    @Test(dataProvider = "Options")
    public void testRepairDBWithCompaction(HaloDBOptions options) throws HaloDBException, InterruptedException, IOException {
        String directory = TestUtils.getTestDirectory("DBRepairTest", "testRepairDBWithCompaction");

        options.setMaxFileSize(1024 * 1024);
        options.setCompactionThresholdPerFile(0.5);
        HaloDB db = getTestDB(directory, options);
        int noOfRecords = 10 * 1024 + 512;

        List<Record> records = TestUtils.insertRandomRecordsOfSize(db, noOfRecords, 1024-Record.Header.HEADER_SIZE);
        List<Record> toUpdate = IntStream.range(0, noOfRecords).filter(i -> i%2==0).mapToObj(i -> records.get(i)).collect(Collectors.toList());
        List<Record> updatedRecords = TestUtils.updateRecords(db, toUpdate);
        for (int i = 0; i < updatedRecords.size(); i++) {
            records.set(i * 2, updatedRecords.get(i));
        }

        TestUtils.waitForCompactionToComplete(db);

        FileTime latestDataFileCreatedTime =
            TestUtils.getFileCreationTime(TestUtils.getLatestDataFile(directory).get());

        db.close();

        // trick the db to think that there was an unclean shutdown.
        DBMetaData dbMetaData = new DBMetaData(dbDirectory);
        dbMetaData.setOpen(true);
        dbMetaData.storeToFile();

        // wait for a second so that the new file will have a different created at time.
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }

        db = getTestDBWithoutDeletingFiles(directory, options);

        // latest file should have been repaired and replaced.
        Assert.assertNotEquals(
            TestUtils.getFileCreationTime(TestUtils.getLatestDataFile(directory).get()),
            latestDataFileCreatedTime
        );

        Assert.assertEquals(db.size(), noOfRecords);
        for (Record r : records) {
            Assert.assertEquals(db.get(r.getKey()), r.getValue());
        }
    }

    @Test
    public void testRepairWithMultipleTombstoneFiles() throws HaloDBException, IOException {
        String directory = TestUtils.getTestDirectory("DBRepairTest", "testRepairWithMultipleTombstoneFiles");

        HaloDBOptions options = new HaloDBOptions();
        options.setCompactionDisabled(true);
        options.setMaxFileSize(320);
        HaloDB db = getTestDB(directory, options);

        int noOfTombstonesPerFile = 10;
        int noOfFiles = 3;
        int noOfRecords = noOfTombstonesPerFile * noOfFiles;

        // Since keyLength was 19 tombstone entry is 32 bytes.
        int keyLength = 19;
        int valueLength = 24;

        List<Record> records = new ArrayList<>();
        for (int i = 0; i < noOfRecords; i++) {
            Record r = new Record(TestUtils.generateRandomByteArray(keyLength), TestUtils.generateRandomByteArray(valueLength));
            records.add(r);
            db.put(r.getKey(), r.getValue());
        }

        for (Record r : records) {
            db.delete(r.getKey());
        }

        File[] tombstoneFiles = FileUtils.listTombstoneFiles(new File(directory));

        FileTime latestDataFileCreatedTime =
            TestUtils.getFileCreationTime(TestUtils.getLatestDataFile(directory).get());
        FileTime latestTombstoneFileCreationTime =
            TestUtils.getFileCreationTime(tombstoneFiles[tombstoneFiles.length-1]);

        db.close();

        // trick the db to think that there was an unclean shutdown.
        DBMetaData dbMetaData = new DBMetaData(dbDirectory);
        dbMetaData.setOpen(true);
        dbMetaData.storeToFile();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }

        db = getTestDBWithoutDeletingFiles(directory, options);

        // latest file should have been repaired and replaced.
        Assert.assertNotEquals(
            TestUtils.getFileCreationTime(TestUtils.getLatestDataFile(directory).get()),
            latestDataFileCreatedTime
        );
        Assert.assertNotEquals(
            TestUtils.getFileCreationTime(tombstoneFiles[tombstoneFiles.length-1]),
            latestTombstoneFileCreationTime
        );


        // other two tombstone files should still be there
        Assert.assertTrue(tombstoneFiles[0].exists());
        Assert.assertTrue(tombstoneFiles[1].exists());

        Assert.assertEquals(db.size(), 0);
        for (int i = 0; i < noOfRecords; i++) {
            Assert.assertNull(db.get(records.get(i).getKey()));
        }
    }
}
