/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Arjun Mannaly
 */
public class DBRepairTest extends TestBase {

    @Test
    public void testRepairDB() throws HaloDBException, IOException {
        String directory = TestUtils.getTestDirectory("DBRepairTest", "testRepairDB");

        HaloDBOptions options = new HaloDBOptions();
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

        File latestDataFile = TestUtils.getLatestDataFile(directory).get();
        File latestTombstoneFile = FileUtils.listTombstoneFiles(new File(directory))[0];

        db.close();

        // trick the db to think that there was an unclean shutdown.
        DBMetaData dbMetaData = new DBMetaData(directory);
        dbMetaData.setOpen(true);
        dbMetaData.storeToFile();

        db = getTestDBWithoutDeletingFiles(directory, options);

        // latest file should have been repaired and deleted. 
        Assert.assertFalse(latestDataFile.exists());
        Assert.assertFalse(latestTombstoneFile.exists());

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

    @Test
    public void testRepairDBWithCompaction() throws HaloDBException, InterruptedException, IOException {
        String directory = TestUtils.getTestDirectory("DBRepairTest", "testRepairDBWithCompaction");

        HaloDBOptions options = new HaloDBOptions();
        options.setMaxFileSize(1024 * 1024);

        HaloDB db = getTestDB(directory, options);
        int noOfRecords = 10 * 1024 + 512;

        List<Record> records = TestUtils.insertRandomRecordsOfSize(db, noOfRecords, 1024-Record.Header.HEADER_SIZE);
        records = TestUtils.updateRecords(db, records);

        TestUtils.waitForCompactionToComplete(db);

        File latestDataFile = TestUtils.getLatestDataFile(directory).get();
        File latestCompactionFile = TestUtils.getLatestCompactionFile(directory).get();

        db.close();

        // trick the db to think that there was an unclean shutdown.
        DBMetaData dbMetaData = new DBMetaData(directory);
        dbMetaData.setOpen(true);
        dbMetaData.storeToFile();

        db = getTestDBWithoutDeletingFiles(directory, options);

        // latest file should have been repaired and deleted.
        Assert.assertFalse(latestDataFile.exists());
        Assert.assertFalse(latestCompactionFile.exists());

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

        db.close();

        File latestDataFile = TestUtils.getLatestDataFile(directory).get();
        File[] tombstoneFiles = FileUtils.listTombstoneFiles(new File(directory));
        File latestTombstoneFile = tombstoneFiles[tombstoneFiles.length-1];

        // trick the db to think that there was an unclean shutdown.
        DBMetaData dbMetaData = new DBMetaData(directory);
        dbMetaData.setOpen(true);
        dbMetaData.storeToFile();

        db = getTestDBWithoutDeletingFiles(directory, options);

        // latest file should have been repaired and deleted.
        Assert.assertFalse(latestDataFile.exists());
        Assert.assertFalse(latestTombstoneFile.exists());

        // other two tombstone files should still be there
        Assert.assertTrue(tombstoneFiles[0].exists());
        Assert.assertTrue(tombstoneFiles[1].exists());

        Assert.assertEquals(db.size(), 0);
        for (int i = 0; i < noOfRecords; i++) {
            Assert.assertNull(db.get(records.get(i).getKey()));
        }
    }
}
