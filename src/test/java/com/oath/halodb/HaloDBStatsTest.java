/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @author Arjun Mannaly
 */
public class HaloDBStatsTest extends TestBase {

    @Test
    public void testOptions() throws HaloDBException {
        String dir = TestUtils.getTestDirectory("HaloDBStatsTest", "testOptions");

        HaloDBOptions options = new HaloDBOptions();
        options.maxFileSize = 10 * 1024;
        options.isCompactionDisabled = true;
        options.compactionThresholdPerFile = 0.9;
        options.flushDataSizeBytes = 1024;
        options.compactionJobRate = 2048;
        options.numberOfRecords = 100;
        options.cleanUpKeyCacheOnClose = true;

        HaloDB db = getTestDB(dir, options);

        HaloDBStats stats = db.stats();
        HaloDBOptions actual = stats.getOptions();

        Assert.assertEquals(actual.maxFileSize, options.maxFileSize);
        Assert.assertEquals(actual.isCompactionDisabled, options.isCompactionDisabled);
        Assert.assertEquals(actual.compactionThresholdPerFile, options.compactionThresholdPerFile);
        Assert.assertEquals(actual.flushDataSizeBytes, options.flushDataSizeBytes);
        Assert.assertEquals(actual.compactionJobRate, options.compactionJobRate);
        Assert.assertEquals(actual.numberOfRecords, options.numberOfRecords);
        Assert.assertEquals(actual.cleanUpKeyCacheOnClose, options.cleanUpKeyCacheOnClose);
        Assert.assertEquals(stats.getSize(), 0);
    }

    @Test
    public void testStaleMap() throws HaloDBException {

        String dir = TestUtils.getTestDirectory("HaloDBStatsTest", "testStaleMap");

        HaloDBOptions options = new HaloDBOptions();
        options.maxFileSize = 10 * 1024;
        options.compactionThresholdPerFile = 0.50;

        HaloDB db = getTestDB(dir, options);

        // will create 10 files with 10 records each. 
        int recordSize = 1024 - Record.Header.HEADER_SIZE;
        List<Record> records = TestUtils.insertRandomRecordsOfSize(db, 100, recordSize);

        // No updates hence stale data map should be empty. 
        Assert.assertEquals(db.stats().getStaleDataPercentPerFile().size(), 0);

        for (int i = 0; i < records.size(); i++) {
            if (i % 10 == 0)
                db.put(records.get(i).getKey(), TestUtils.generateRandomByteArray(recordSize));
        }

        // Updated 1 out of 10 records in each file, hence 10% stale data. 
        Assert.assertEquals(db.stats().getStaleDataPercentPerFile().size(), 10);
        db.stats().getStaleDataPercentPerFile().forEach((k, v) -> {
            Assert.assertEquals(v, 10.0);
        });

        Assert.assertEquals(db.stats().getSize(), 100);
    }

    @Test
    public void testCompactionStats() throws HaloDBException {

        String dir = TestUtils.getTestDirectory("HaloDBStatsTest", "testCompactionStats");

        HaloDBOptions options = new HaloDBOptions();
        options.maxFileSize = 10 * 1024;
        options.compactionThresholdPerFile = 0.50;
        options.isCompactionDisabled = true;

        HaloDB db = getTestDB(dir, options);
        // will create 10 files with 10 records each.
        int recordSize = 1024 - Record.Header.HEADER_SIZE;
        int noOfRecords = 100;
        List<Record> records = TestUtils.insertRandomRecordsOfSize(db, noOfRecords, recordSize);

        // update 50% of records in each file. 
        for (int i = 0; i < records.size(); i++) {
            if (i % 10 < 5)
                db.put(records.get(i).getKey(), TestUtils.generateRandomByteArray(recordSize));
        }

        db.close();

        options.isCompactionDisabled = false;
        db = getTestDBWithoutDeletingFiles(dir, options);

        TestUtils.waitForCompactionToComplete(db);

        // compaction complete hence stale data map is empty. 
        HaloDBStats stats = db.stats();
        Assert.assertEquals(stats.getStaleDataPercentPerFile().size(), 0);

        Assert.assertEquals(stats.getNumberOfFilesPendingCompaction(), 0);
        Assert.assertEquals(stats.getNumberOfRecordsCopied(), noOfRecords / 2);
        Assert.assertEquals(stats.getNumberOfRecordsReplaced(), noOfRecords / 2);
        Assert.assertEquals(stats.getNumberOfRecordsScanned(), noOfRecords);
        Assert.assertEquals(stats.getSizeOfRecordsCopied(), noOfRecords / 2 * 1024);
        Assert.assertEquals(stats.getSizeOfFilesDeleted(), options.maxFileSize * 10);
        Assert.assertEquals(stats.getSizeReclaimed(), options.maxFileSize * 10 / 2);
        Assert.assertEquals(stats.getSize(), noOfRecords);

        db.resetStats();
        stats = db.stats();
        Assert.assertEquals(stats.getNumberOfFilesPendingCompaction(), 0);
        Assert.assertEquals(stats.getNumberOfRecordsCopied(), 0);
        Assert.assertEquals(stats.getNumberOfRecordsReplaced(), 0);
        Assert.assertEquals(stats.getNumberOfRecordsScanned(), 0);
        Assert.assertEquals(stats.getSizeOfRecordsCopied(), 0);
        Assert.assertEquals(stats.getSizeOfFilesDeleted(), 0);
        Assert.assertEquals(stats.getSizeReclaimed(), 0);
        Assert.assertEquals(stats.getSize(), noOfRecords);
    }

    @Test
    public void testKeyCacheStats() throws HaloDBException {
        String dir = TestUtils.getTestDirectory("HaloDBStatsTest", "testKeyCacheStats");

        int numberOfSegments = (int)Utils.roundUpToPowerOf2(Runtime.getRuntime().availableProcessors() * 2);
        int numberOfRecords = numberOfSegments * 1024;

        HaloDBOptions options = new HaloDBOptions();
        options.maxFileSize = 10 * 1024;
        options.numberOfRecords = numberOfRecords;
        options.compactionThresholdPerFile = 0.50;
        options.isCompactionDisabled = true;

        HaloDB db = getTestDB(dir, options);
        HaloDBStats stats = db.stats();
        Assert.assertEquals(numberOfSegments, stats.getNumberOfSegments());
        Assert.assertEquals(numberOfRecords/numberOfSegments, stats.getMaxSizePerSegment());

        long[] expected = new long[numberOfSegments];
        Arrays.fill(expected, 0);
        Assert.assertEquals(stats.getCountPerSegment(), expected);
    }
}
