/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

/**
 * @author Arjun Mannaly
 */
public class DBRepairTest extends TestBase {

    @Test
    public void testRepairDB() throws HaloDBException, IOException {
        String directory = Paths.get("tmp", "DBRepairTest", "testRepairDB").toString();

        HaloDBOptions options = new HaloDBOptions();
        options.setMaxFileSize(1024 * 1024);

        HaloDB db = getTestDB(directory, options);
        int noOfRecords = 5 * 1024 + 512; // 5 files with 1024 records and 1 with 512 records. 

        List<Record> records = TestUtils.insertRandomRecordsOfSize(db, noOfRecords, 1024-Record.Header.HEADER_SIZE);

        File latestDataFile = TestUtils.getLatestDataFile(directory).get();

        db.close();

        // trick the db to think that there was an unclean shutdown.
        DBMetaData dbMetaData = new DBMetaData(directory);
        dbMetaData.setOpen(true);
        dbMetaData.storeToFile();

        db = getTestDBWithoutDeletingFiles(directory, options);

        // latest file should have been repaired and deleted. 
        Assert.assertFalse(latestDataFile.exists());

        Assert.assertEquals(db.size(), noOfRecords);
        for (Record r : records) {
            Assert.assertEquals(db.get(r.getKey()), r.getValue());
        }
    }

    @Test
    public void testRepairDBWithCompaction() throws HaloDBException, InterruptedException, IOException {
        String directory = Paths.get("tmp", "DBRepairTest", "testRepairDBWithCompaction").toString();

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
}
