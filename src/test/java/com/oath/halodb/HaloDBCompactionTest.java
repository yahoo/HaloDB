/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import com.google.common.primitives.Longs;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Arjun Mannaly
 */
public class HaloDBCompactionTest extends TestBase {

    private final int recordSize = 1024;
    private final int numberOfFiles = 8;
    private final int recordsPerFile = 10;
    private final int numberOfRecords = numberOfFiles * recordsPerFile;

    @Test(dataProvider = "Options")
    public void testCompaction(HaloDBOptions options) throws Exception {
        String directory = TestUtils.getTestDirectory("HaloDBCompactionTest", "testCompaction");

        options.setMaxFileSize(recordsPerFile * recordSize);
        options.setCompactionThresholdPerFile(0.5);
        options.setCompactionDisabled(false);
        options.setFlushDataSizeBytes(2048);

        HaloDB db =  getTestDB(directory, options);

        Record[] records = insertAndUpdateRecords(numberOfRecords, db);

        TestUtils.waitForCompactionToComplete(db);

        for (Record r : records) {
            byte[] actual = db.get(r.getKey());
            Assert.assertEquals(r.getValue(), actual);
        }
    }

    @Test(dataProvider = "Options")
    public void testReOpenDBAfterMerge(HaloDBOptions options) throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBCompactionTest", "testReOpenDBAfterMerge");

        options.setMaxFileSize(recordsPerFile * recordSize);
        options.setCompactionThresholdPerFile(0.5);
        options.setCompactionDisabled(false);

        HaloDB db = getTestDB(directory, options);

        Record[] records = insertAndUpdateRecords(numberOfRecords, db);

        TestUtils.waitForCompactionToComplete(db);

        db.close();

        db = getTestDBWithoutDeletingFiles(directory, options);

        for (Record r : records) {
            byte[] actual = db.get(r.getKey());
            Assert.assertEquals(actual, r.getValue());
        }
    }

    @Test(dataProvider = "Options")
    public void testReOpenDBWithoutMerge(HaloDBOptions options) throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBCompactionTest", "testReOpenAndUpdatesAndWithoutMerge");

        options.setMaxFileSize(recordsPerFile * recordSize);
        options.setCompactionDisabled(true);

        HaloDB db = getTestDB(directory, options);

        Record[] records = insertAndUpdateRecords(numberOfRecords, db);

        db.close();

        db = getTestDBWithoutDeletingFiles(directory, options);

        for (Record r : records) {
            byte[] actual = db.get(r.getKey());
            Assert.assertEquals(actual, r.getValue());
        }
    }

    @Test(dataProvider = "Options")
    public void testSyncWrites(HaloDBOptions options) throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBCompactionTest", "testSyncWrites");
        options.enableSyncWrites(true);
        HaloDB db = getTestDB(directory, options);
        List<Record> records = TestUtils.insertRandomRecords(db, 10_000);
        List<Record> current = new ArrayList<>();
        for (int i = 0; i < records.size(); i++) {
            if (i % 2 == 0) {
                db.delete(records.get(i).getKey());
            }
            else {
                current.add(records.get(i));
            }
        }
        db.close();

        db = getTestDBWithoutDeletingFiles(directory, options);
        Assert.assertEquals(db.size(), current.size());
        for (Record r : current) {
            Assert.assertEquals(db.get(r.getKey()), r.getValue());
        }
    }

    @Test(dataProvider = "Options")
    public void testUpdatesToSameFile(HaloDBOptions options) throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBCompactionTest", "testUpdatesToSameFile");

        options.setMaxFileSize(recordsPerFile * recordSize);
        options.setCompactionDisabled(true);

        HaloDB db = getTestDB(directory, options);

        Record[] records = insertAndUpdateRecordsToSameFile(2, db);

        db.close();

        db = getTestDBWithoutDeletingFiles(directory, options);

        for (Record r : records) {
            byte[] actual = db.get(r.getKey());
            Assert.assertEquals(actual, r.getValue());
        }
    }

    @Test(dataProvider = "Options")
    public void testFilesWithStaleDataAddedToCompactionQueueDuringDBOpen(HaloDBOptions options) throws HaloDBException, InterruptedException {
        String directory = TestUtils.getTestDirectory("HaloDBCompactionTest", "testFilesWithStaleDataAddedToCompactionQueueDuringDBOpen");

        options.setCompactionDisabled(true);
        options.setMaxFileSize(10 * 1024);

        HaloDB db = getTestDB(directory, options);

        // insert 50 records into 5 files.
        List<Record> records = TestUtils.insertRandomRecordsOfSize(db, 50, 1024-Record.Header.HEADER_SIZE);

        // Delete all records, which means that all data files would have crossed the
        // stale data threshold.  
        for (Record r : records) {
            db.delete(r.getKey());
        }

        db.close();

        // open the db withe compaction enabled. 
        options.setCompactionDisabled(false);
        options.setMaxFileSize(10 * 1024);

        db = getTestDBWithoutDeletingFiles(directory, options);

        TestUtils.waitForCompactionToComplete(db);

        // Since all files have crossed stale data threshold, everything will be compacted and deleted.
        Assert.assertFalse(TestUtils.getLatestDataFile(directory).isPresent());
        Assert.assertFalse(TestUtils.getLatestCompactionFile(directory).isPresent());

        db.close();

        // open the db with compaction disabled.
        options.setMaxFileSize(10 * 1024);
        options.setCompactionDisabled(true);

        db = getTestDBWithoutDeletingFiles(directory, options);

        // insert 20 records into two files. 
        records = TestUtils.insertRandomRecordsOfSize(db, 20, 1024-Record.Header.HEADER_SIZE);
        File[] dataFilesToDelete = FileUtils.listDataFiles(new File(directory));

        // update all records; since compaction is disabled no file is deleted.
        List<Record> updatedRecords = TestUtils.updateRecords(db, records);

        db.close();

        // Open db again with compaction enabled.
        options.setCompactionDisabled(false);
        options.setMaxFileSize(10 * 1024);

        db = getTestDBWithoutDeletingFiles(directory, options);
        TestUtils.waitForCompactionToComplete(db);

        //Confirm that previous data files were compacted and deleted.
        for (File f : dataFilesToDelete) {
            Assert.assertFalse(f.exists());
        }

        for (Record r : updatedRecords) {
            Assert.assertEquals(db.get(r.getKey()), r.getValue());
        }
    }

    private Record[] insertAndUpdateRecords(int numberOfRecords, HaloDB db) throws HaloDBException {
        int valueSize = recordSize - Record.Header.HEADER_SIZE - 8; // 8 is the key size.

        Record[] records = new Record[numberOfRecords];
        for (int i = 0; i < numberOfRecords; i++) {
            byte[] key = Longs.toByteArray(i);
            byte[] value = TestUtils.generateRandomByteArray(valueSize);
            records[i] = new Record(key, value);
            db.put(records[i].getKey(), records[i].getValue());
        }

        // modify first 5 records of each file.
        byte[] modifiedMark = "modified".getBytes();
        for (int k = 0; k < numberOfFiles; k++) {
            for (int i = 0; i < 5; i++) {
                Record r = records[i + k*10];
                byte[] value = r.getValue();
                System.arraycopy(modifiedMark, 0, value, 0, modifiedMark.length);
                Record modifiedRecord = new Record(r.getKey(), value);
                records[i + k*10] = modifiedRecord;
                db.put(modifiedRecord.getKey(), modifiedRecord.getValue());
            }
        }
        return records;
    }

    private Record[] insertAndUpdateRecordsToSameFile(int numberOfRecords, HaloDB db) throws HaloDBException {
        int valueSize = recordSize - Record.Header.HEADER_SIZE - 8; // 8 is the key size.

        Record[] records = new Record[numberOfRecords];
        for (int i = 0; i < numberOfRecords; i++) {
            byte[] key = Longs.toByteArray(i);
            byte[] value = TestUtils.generateRandomByteArray(valueSize);

            byte[] updatedValue = null;
            for (long j = 0; j < recordsPerFile; j++) {
                updatedValue = TestUtils.concatenateArrays(value, Longs.toByteArray(i));
                db.put(key, updatedValue);
            }

            // only store the last updated valued.
            records[i] = new Record(key, updatedValue);
        }

        return records;
    }
}
