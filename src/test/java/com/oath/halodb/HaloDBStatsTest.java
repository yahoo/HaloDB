/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

public class HaloDBStatsTest extends TestBase {

    @Test(dataProvider = "Options")
    public void testOptions(HaloDBOptions options) throws HaloDBException {
        String dir = TestUtils.getTestDirectory("HaloDBStatsTest", "testOptions");

        options.setMaxFileSize(10 * 1024);
        options.setCompactionDisabled(true);
        options.setCompactionThresholdPerFile(0.9);
        options.setFlushDataSizeBytes(1024);
        options.setCompactionJobRate(2048);
        options.setNumberOfRecords(100);
        options.setCleanUpInMemoryIndexOnClose(true);

        HaloDB db = getTestDB(dir, options);

        HaloDBStats stats = db.stats();
        HaloDBOptions actual = stats.getOptions();

        Assert.assertEquals(actual.getMaxFileSize(), options.getMaxFileSize());
        Assert.assertEquals(actual.getCompactionThresholdPerFile(), options.getCompactionThresholdPerFile());
        Assert.assertEquals(actual.getFlushDataSizeBytes(), options.getFlushDataSizeBytes());
        Assert.assertEquals(actual.getCompactionJobRate(), options.getCompactionJobRate());
        Assert.assertEquals(actual.getNumberOfRecords(), options.getNumberOfRecords());
        Assert.assertEquals(actual.isCleanUpInMemoryIndexOnClose(), options.isCleanUpInMemoryIndexOnClose());
        Assert.assertEquals(stats.getSize(), 0);
    }

    @Test(dataProvider = "Options")
    public void testStaleMap(HaloDBOptions options) throws HaloDBException {

        String dir = TestUtils.getTestDirectory("HaloDBStatsTest", "testStaleMap");

        options.setMaxFileSize(10 * 1024);
        options.setCompactionThresholdPerFile(0.50);

        HaloDB db = getTestDB(dir, options);

        // will create 10 files with 10 records each.
        int recordSize = 1024 - RecordEntry.Header.HEADER_SIZE;
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

    @Test(dataProvider = "Options")
    public void testCompactionStats(HaloDBOptions options) throws HaloDBException {

        String dir = TestUtils.getTestDirectory("HaloDBStatsTest", "testCompactionStats");

        options.setMaxFileSize(10 * 1024);
        options.setCompactionThresholdPerFile(0.50);
        options.setCompactionDisabled(true);

        HaloDB db = getTestDB(dir, options);
        // will create 10 files with 10 records each.
        int recordSize = 1024 - RecordEntry.Header.HEADER_SIZE;
        int noOfRecords = 100;
        List<Record> records = TestUtils.insertRandomRecordsOfSize(db, noOfRecords, recordSize);

        Assert.assertEquals(db.stats().getNumberOfDataFiles(), 10);
        Assert.assertEquals(db.stats().getNumberOfTombstoneFiles(), 0);

        // update 50% of records in each file.
        for (int i = 0; i < records.size(); i++) {
            if (i % 10 < 5)
                db.put(records.get(i).getKey(), TestUtils.generateRandomByteArray(records.get(i).getValue().length));
        }

        TestUtils.waitForCompactionToComplete(db);

        // compaction stats are 0 since compaction is paused.
        Assert.assertFalse(db.stats().isCompactionRunning());
        Assert.assertEquals(db.stats().getCompactionRateInInternal(), 0);
        Assert.assertEquals(db.stats().getCompactionRateSinceBeginning(), 0);
        Assert.assertNotEquals(db.stats().toString().length(), 0);
        Assert.assertEquals(db.stats().getNumberOfDataFiles(), 15);
        Assert.assertEquals(db.stats().getNumberOfTombstoneFiles(), 0);

        db.close();

        options.setCompactionDisabled(false);
        db = getTestDBWithoutDeletingFiles(dir, options);
        Assert.assertTrue(db.stats().isCompactionRunning());

        TestUtils.waitForCompactionToComplete(db);

        // compaction complete hence stale data map is empty.
        HaloDBStats stats = db.stats();
        Assert.assertEquals(stats.getStaleDataPercentPerFile().size(), 0);

        Assert.assertEquals(stats.getNumberOfFilesPendingCompaction(), 0);
        Assert.assertEquals(stats.getNumberOfRecordsCopied(), noOfRecords / 2);
        Assert.assertEquals(stats.getNumberOfRecordsReplaced(), noOfRecords / 2);
        Assert.assertEquals(stats.getNumberOfRecordsScanned(), noOfRecords);
        Assert.assertEquals(stats.getSizeOfRecordsCopied(), noOfRecords / 2 * 1024);
        Assert.assertEquals(stats.getSizeOfFilesDeleted(), options.getMaxFileSize() * 10);
        Assert.assertEquals(stats.getSizeReclaimed(), options.getMaxFileSize() * 10 / 2);
        Assert.assertEquals(stats.getSize(), noOfRecords);
        Assert.assertNotEquals(db.stats().getCompactionRateInInternal(), 0);
        Assert.assertNotEquals(db.stats().getCompactionRateSinceBeginning(), 0);
        Assert.assertNotEquals(db.stats().toString().length(), 0);
        Assert.assertEquals(db.stats().getNumberOfDataFiles(), 10);
        Assert.assertEquals(db.stats().getNumberOfTombstoneFiles(), 0);

        // delete the 50% of records
        for (int i = 0; i < records.size(); i++) {
            if (i % 10 < 5)
                db.delete(records.get(i).getKey());
        }

        // restart compaction thread because it was stopped by TestUtils.waitForCompactionToComplete(db)
        db.resumeCompaction();

        TestUtils.waitForCompactionToComplete(db);
        Assert.assertEquals(db.stats().getNumberOfDataFiles(), 5);
        Assert.assertEquals(db.stats().getNumberOfTombstoneFiles(), 1);

        db.close();

        // all delete records have been removed by compaction job, so no record copied at reopen
        options.setCleanUpTombstonesDuringOpen(true);
        db = getTestDBWithoutDeletingFiles(dir, options);
        stats = db.stats();
        Assert.assertEquals(stats.getSize(), noOfRecords / 2);
        Assert.assertEquals(db.stats().getNumberOfTombstonesCleanedUpDuringOpen(), noOfRecords / 2);
        Assert.assertEquals(db.stats().getNumberOfTombstonesFoundDuringOpen(), noOfRecords / 2);
        Assert.assertEquals(db.stats().getNumberOfDataFiles(), 5);
        Assert.assertEquals(db.stats().getNumberOfTombstoneFiles(), 0);

        db.resetStats();
        stats = db.stats();
        Assert.assertEquals(stats.getNumberOfFilesPendingCompaction(), 0);
        Assert.assertEquals(stats.getNumberOfRecordsCopied(), 0);
        Assert.assertEquals(stats.getNumberOfRecordsReplaced(), 0);
        Assert.assertEquals(stats.getNumberOfRecordsScanned(), 0);
        Assert.assertEquals(stats.getSizeOfRecordsCopied(), 0);
        Assert.assertEquals(stats.getSizeOfFilesDeleted(), 0);
        Assert.assertEquals(stats.getSizeReclaimed(), 0);
        Assert.assertEquals(stats.getSize(), noOfRecords / 2);
        Assert.assertEquals(db.stats().getCompactionRateInInternal(), 0);
        Assert.assertNotEquals(db.stats().getCompactionRateSinceBeginning(), 0);
        Assert.assertNotEquals(db.stats().toString().length(), 0);
    }

    @Test(dataProvider = "Options")
    public void testIndexStats(HaloDBOptions options) throws HaloDBException {
        String dir = TestUtils.getTestDirectory("HaloDBStatsTest", "testIndexStats");

        int numberOfSegments = (int)Utils.roundUpToPowerOf2(Runtime.getRuntime().availableProcessors() * 2);
        int numberOfRecords = numberOfSegments * 1024;

        options.setMaxFileSize(10 * 1024);
        options.setNumberOfRecords(numberOfRecords);
        options.setCompactionThresholdPerFile(0.50);
        options.setCompactionDisabled(true);

        HaloDB db = getTestDB(dir, options);
        HaloDBStats stats = db.stats();
        Assert.assertEquals(numberOfSegments, stats.getNumberOfSegments());
        Assert.assertEquals(numberOfRecords/numberOfSegments, stats.getMaxSizePerSegment());

        SegmentStats[] expected = new SegmentStats[numberOfSegments];
        SegmentStats s;
        if (options.isUseMemoryPool()) {
            s = new SegmentStats(0, 0, 0, 0);
        }
        else {
            s = new SegmentStats(0, -1, -1, -1);
        }

        Arrays.fill(expected, s);
        Assert.assertEquals(stats.getSegmentStats(), expected);
    }

    @Test(dataProvider = "Options")
    public void testStatsToStringMap(HaloDBOptions options) throws HaloDBException {
        String dir = TestUtils.getTestDirectory("HaloDBStatsTest", "testStatsToStringMap");

        HaloDB db = getTestDB(dir, options);

        HaloDBStats stats = db.stats();
        Map<String, String> map = stats.toStringMap();
        Assert.assertEquals(map.size(), 22);
        Assert.assertNotNull(map.get("statsResetTime"));
        Assert.assertNotNull(map.get("size"));
        Assert.assertNotNull(map.get("Options"));
        Assert.assertNotNull(map.get("isCompactionRunning"));
        Assert.assertNotNull(map.get("CompactionJobRateInInterval"));
        Assert.assertNotNull(map.get("CompactionJobRateSinceBeginning"));
        Assert.assertNotNull(map.get("numberOfFilesPendingCompaction"));
        Assert.assertNotNull(map.get("numberOfRecordsCopied"));
        Assert.assertNotNull(map.get("numberOfRecordsReplaced"));
        Assert.assertNotNull(map.get("numberOfRecordsScanned"));
        Assert.assertNotNull(map.get("sizeOfRecordsCopied"));
        Assert.assertNotNull(map.get("sizeOfFilesDeleted"));
        Assert.assertNotNull(map.get("sizeReclaimed"));
        Assert.assertNotNull(map.get("rehashCount"));
        Assert.assertNotNull(map.get("maxSizePerSegment"));
        Assert.assertNotNull(map.get("numberOfDataFiles"));
        Assert.assertNotNull(map.get("numberOfTombstoneFiles"));
        Assert.assertNotNull(map.get("numberOfTombstonesFoundDuringOpen"));
        Assert.assertNotNull(map.get("numberOfTombstonesCleanedUpDuringOpen"));
        Assert.assertNotNull(map.get("segmentStats"));
        Assert.assertNotNull(map.get("numberOfSegments"));
        Assert.assertNotNull(map.get("staleDataPercentPerFile"));
    }

}
