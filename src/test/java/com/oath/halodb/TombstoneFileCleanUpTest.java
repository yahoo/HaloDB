package com.oath.halodb;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class TombstoneFileCleanUpTest extends TestBase {

    @Test(dataProvider = "Options")
    public void testDeleteAllRecords(HaloDBOptions options) throws HaloDBException, IOException {
        String directory = TestUtils.getTestDirectory("TombstoneFileCleanUpTest", "testDeleteAllRecords");

        options.setCleanUpTombstonesDuringOpen(true);
        options.setCompactionThresholdPerFile(1);
        options.setMaxFileSize(1024 * 1024);
        HaloDB db = getTestDB(directory, options);

        // 1024 records in 100 files.
        int noOfRecordsPerFile = 1024;
        int noOfFiles = 100;
        int noOfRecords = noOfRecordsPerFile * noOfFiles;
        List<Record> records = TestUtils.insertRandomRecordsOfSize(db, noOfRecords, 1024-Record.Header.HEADER_SIZE);

        // delete all records
        for (Record r : records) {
            db.delete(r.getKey());
        }

        // all files will be deleted except for the last one as it is the current write file. 
        TestUtils.waitForCompactionToComplete(db);

        // close and open the db.
        db.close();
        db = getTestDBWithoutDeletingFiles(directory, options);

        // Since we waited for compaction to complete all but one file must be deleted.
        // Therefore, there will be one tombstone file with 1024 records from the last data file.
        File[] tombstoneFiles = FileUtils.listTombstoneFiles(new File(directory));
        Assert.assertEquals(tombstoneFiles.length, 1);
        TombstoneFile tombstoneFile = new TombstoneFile(tombstoneFiles[0], options, dbDirectory);
        tombstoneFile.open();
        TombstoneFile.TombstoneFileIterator iterator = tombstoneFile.newIterator();

        //Make sure that only 1024 tombstones from the last data file are left in the tombstone file after clean up.  
        int tombstoneCount = 0;
        List<Record> remaining = records.stream().skip(noOfRecords - noOfRecordsPerFile).collect(Collectors.toList());
        while (iterator.hasNext()) {
            TombstoneEntry entry = iterator.next();
            Assert.assertEquals(entry.getKey(), remaining.get(tombstoneCount++).getKey());
        }

        Assert.assertEquals(tombstoneCount, noOfRecordsPerFile);

        HaloDBStats stats = db.stats();
        Assert.assertEquals(noOfRecords, stats.getNumberOfTombstonesFoundDuringOpen());
        Assert.assertEquals(noOfRecords - noOfRecordsPerFile, stats.getNumberOfTombstonesCleanedUpDuringOpen());
    }

    @Test(dataProvider = "Options")
    public void testDeleteAndInsertRecords(HaloDBOptions options) throws IOException, HaloDBException {
        String directory = TestUtils.getTestDirectory("TombstoneFileCleanUpTest", "testDeleteAndInsertRecords");

        options.setCleanUpTombstonesDuringOpen(true);
        options.setCompactionThresholdPerFile(1);
        HaloDB db = getTestDB(directory, new HaloDBOptions());

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        // delete all records
        for (Record r : records) {
            db.delete(r.getKey());
        }

        // insert records again.
        for (Record r : records) {
            db.put(r.getKey(), r.getValue());
        }

        TestUtils.waitForCompactionToComplete(db);

        db.close();

        // all records were written again after deleting, therefore all tombstone records should be deleted.
        db = getTestDBWithoutDeletingFiles(directory, options);

        Assert.assertEquals(FileUtils.listTombstoneFiles(new File(directory)).length, 0);

        HaloDBStats stats = db.stats();
        Assert.assertEquals(noOfRecords, stats.getNumberOfTombstonesFoundDuringOpen());
        Assert.assertEquals(noOfRecords, stats.getNumberOfTombstonesCleanedUpDuringOpen());
    }

    @Test(dataProvider = "Options")
    public void testDeleteRecordsWithoutCompaction(HaloDBOptions options) throws IOException, HaloDBException {
        String directory = TestUtils.getTestDirectory("TombstoneFileCleanUpTest", "testDeleteRecordsWithoutCompaction");

        options.setMaxFileSize(1024 * 1024);
        options.setCompactionThresholdPerFile(1);
        options.setCleanUpTombstonesDuringOpen(true);
        HaloDB db = getTestDB(directory, options);

        // 1024 records in 100 files.
        int noOfRecordsPerFile = 1024;
        int noOfFiles = 100;
        int noOfRecords = noOfRecordsPerFile * noOfFiles;
        List<Record> records = TestUtils.insertRandomRecordsOfSize(db, noOfRecords, 1024-Record.Header.HEADER_SIZE);

        // delete first record from each file, since compaction threshold is 1 none of the files will be compacted.
        for (int i = 0; i < noOfRecords; i+=noOfRecordsPerFile) {
            db.delete(records.get(i).getKey());
        }

        // get the tombstone file. 
        File[] originalTombstoneFiles = FileUtils.listTombstoneFiles(new File(directory));
        Assert.assertEquals(originalTombstoneFiles.length, 1);

        TestUtils.waitForCompactionToComplete(db);

        // close and open db. 
        db.close();
        db = getTestDBWithoutDeletingFiles(directory, options);

        // make sure that the old tombstone file was deleted. 
        Assert.assertFalse(originalTombstoneFiles[0].exists());

        // Since none of the files were compacted we cannot delete any of the tombstone records
        // as the stale version of records still exist in the db. 

        // find the new tombstone file and make sure that all the tombstone records were copied.
        File[] tombstoneFilesAfterOpen = FileUtils.listTombstoneFiles(new File(directory));
        Assert.assertEquals(tombstoneFilesAfterOpen.length, 1);
        Assert.assertNotEquals(tombstoneFilesAfterOpen[0].getName(), originalTombstoneFiles[0].getName());

        TombstoneFile tombstoneFile = new TombstoneFile(tombstoneFilesAfterOpen[0], options, dbDirectory);
        tombstoneFile.open();
        TombstoneFile.TombstoneFileIterator iterator = tombstoneFile.newIterator();

        int tombstoneCount = 0;
        while (iterator.hasNext()) {
            TombstoneEntry entry = iterator.next();
            Assert.assertEquals(entry.getKey(), records.get(tombstoneCount*1024).getKey());
            tombstoneCount++;
        }
        Assert.assertEquals(tombstoneCount, noOfFiles);

        HaloDBStats stats = db.stats();
        Assert.assertEquals(noOfFiles, stats.getNumberOfTombstonesFoundDuringOpen());
        Assert.assertEquals(0, stats.getNumberOfTombstonesCleanedUpDuringOpen());
    }

    @Test(dataProvider = "Options")
    public void testWithCleanUpTurnedOff(HaloDBOptions options) throws IOException, HaloDBException {
        String directory = TestUtils.getTestDirectory("TombstoneFileCleanUpTest", "testWithCleanUpTurnedOff");

        options.setCleanUpTombstonesDuringOpen(false);
        options.setCompactionThresholdPerFile(1);
        options.setMaxFileSize(1024 * 1024);
        HaloDB db = getTestDB(directory, new HaloDBOptions());

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        // delete all records
        for (Record r : records) {
            db.delete(r.getKey());
        }

        // insert records again.
        for (Record r : records) {
            db.put(r.getKey(), r.getValue());
        }

        // get the tombstone file.
        File[] originalTombstoneFiles = FileUtils.listTombstoneFiles(new File(directory));
        Assert.assertEquals(originalTombstoneFiles.length, 1);

        TestUtils.waitForCompactionToComplete(db);

        db.close();
        db = getTestDBWithoutDeletingFiles(directory, options);

        // clean up was disabled; tombstone file should be the same. 
        File[] tombstoneFilesAfterOpen = FileUtils.listTombstoneFiles(new File(directory));
        Assert.assertEquals(tombstoneFilesAfterOpen.length, 1);
        Assert.assertEquals(tombstoneFilesAfterOpen[0].getName(), originalTombstoneFiles[0].getName());

        HaloDBStats stats = db.stats();
        Assert.assertEquals(noOfRecords, stats.getNumberOfTombstonesFoundDuringOpen());
        Assert.assertEquals(0, stats.getNumberOfTombstonesCleanedUpDuringOpen());
    }

    @Test(dataProvider = "Options")
    public void testCopyMultipleTombstoneFiles(HaloDBOptions options) throws HaloDBException, IOException {
        //Test to make sure that rollover to tombstone files work correctly during cleanup. 

        String directory = TestUtils.getTestDirectory("TombstoneFileCleanUpTest", "testCopyMultipleTombstoneFiles");

        options.setCleanUpTombstonesDuringOpen(true);
        options.setCompactionDisabled(true);
        options.setMaxFileSize(512);
        HaloDB db = getTestDB(directory, options);

        int noOfRecordsPerFile = 8;
        int noOfFiles = 8;
        int noOfRecords = noOfRecordsPerFile * noOfFiles;

        int keyLength = 18;
        int valueLength = 24;

        List<Record> records = new ArrayList<>();
        for (int i = 0; i < noOfRecords; i++) {
            Record r = new Record(TestUtils.generateRandomByteArray(keyLength), TestUtils.generateRandomByteArray(valueLength));
            records.add(r);
            db.put(r.getKey(), r.getValue());
        }

        for (int i = 0; i < noOfRecords/2; i++) {
            Record r = records.get(i);
            db.delete(r.getKey());
        }

        // close and open the db.
        db.close();
        db = getTestDBWithoutDeletingFiles(directory, options);

        // Since keyLength was 18, plus header length 14, tombstone entry is 32 bytes.
        // Since file size is 512 there would be two tombstone files both of which should be copied.
        File[] tombstoneFiles = FileUtils.listTombstoneFiles(new File(directory));
        Set<String> tombstones = new HashSet<>();
        Assert.assertEquals(tombstoneFiles.length, 2);
        for (File f : tombstoneFiles) {
            TombstoneFile tombstoneFile = new TombstoneFile(f, options, dbDirectory);
            tombstoneFile.open();
            TombstoneFile.TombstoneFileIterator iterator = tombstoneFile.newIterator();
            while (iterator.hasNext()) {
                tombstones.add(Arrays.toString(iterator.next().getKey()));
            }
        }

        for (int i = 0; i < tombstones.size(); i++) {
            Assert.assertTrue(tombstones.contains(Arrays.toString(records.get(i).getKey())));
        }

        HaloDBStats stats = db.stats();
        Assert.assertEquals(noOfRecords/2, stats.getNumberOfTombstonesFoundDuringOpen());
        Assert.assertEquals(0, stats.getNumberOfTombstonesCleanedUpDuringOpen());
    }

    @Test(dataProvider = "Options")
    public void testMergeTombstoneFiles(HaloDBOptions options) throws IOException, HaloDBException {
        String directory = TestUtils.getTestDirectory("TombstoneFileCleanUpTest", "testMergeTombstoneFiles");

        options.setCompactionThresholdPerFile(0.4);
        options.setMaxFileSize(16 * 1024);
        options.setMaxTombstoneFileSize(2 * 1024);
        HaloDB db = getTestDB(directory, options);

        // Record size: header 18 + key 18 + value 28 = 64 bytes
        // Tombstone entry size: header 14 + key 18 = 32 bytes
        // Each data file will store 16 * 1024 / 64 = 256 records
        // Each tombstone file will store 2 * 1024 / 32 = 64 entries
        // Total data files 2048 / 256 = 8
        // Total tombstone original file count (1024 / 2 + 1024 / 4) / 64 = 12
        // After cleanup, tombstone file count 1024 / 4 / 64 = 4
        int keyLength = 18;
        int valueLength = 28;
        int noOfRecords = 2048;
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < noOfRecords; i++) {
            Record r = new Record(TestUtils.generateRandomByteArray(keyLength), TestUtils.generateRandomByteArray(valueLength));
            records.add(r);
            db.put(r.getKey(), r.getValue());
        }

        // The deletion strategy is:
        // Delete total 1/2 records from first half and 1/4 from second half in turn
        // so that each tombstone file contains entries from both parts
        // Because first half has 50% records deleted, the files which hold first
        // half records will be compacted and tombstone entries will be inactive
        // Tombstone entries of second part are still active
        int mid = records.size() / 2;
        for (int i = 0; i < mid; i++) {
            if (i % 2 == 0) {
                db.delete(records.get(i).getKey());
            }
            if (i % 4 == 0) {
                db.delete((records.get(i+mid).getKey()));
            }
        }
        TestUtils.waitForCompactionToComplete(db);

        db.close();

        File[] original = FileUtils.listTombstoneFiles(new File(directory));
        // See comments above how 12 is calculated
        Assert.assertEquals(original.length, 12);

        // disable CleanUpTombstonesDuringOpen, all original tombstone files preserved
        options.setCleanUpTombstonesDuringOpen(false);
        db = getTestDBWithoutDeletingFiles(directory, options);

        File[] current = FileUtils.listTombstoneFiles(new File(directory));
        Assert.assertEquals(current.length, original.length);
        for (int i = 0; i < original.length; i++) {
            Assert.assertEquals(current[i].getName(), original[i].getName());
        }

        db.close();
        options.setCleanUpTombstonesDuringOpen(true);
        db = getTestDBWithoutDeletingFiles(directory, options);

        original = current;
        current = FileUtils.listTombstoneFiles(new File(directory));
        // See comments above how 4 is calculated
        Assert.assertEquals(current.length, 4);
        // All tombstone files are rolled over to new files
        Assert.assertTrue(getFileId(current[0].getName()) > getFileId(original[original.length-1].getName()));

        // Delete 1 record and verify currentTombstoneFile is initialized
        db.delete(records.get(1).getKey());
        original = current;
        current = FileUtils.listTombstoneFiles(new File(directory));
        Assert.assertEquals(current.length, original.length + 1);
        db.close();
    }

    private int getFileId(String fileName) {
        return Integer.parseInt(fileName.substring(0, fileName.indexOf(".")));
    }
}
