package com.oath.halodb;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Arjun Mannaly
 */
public class TombstoneFileCleanUpTest extends TestBase {

    @Test
    public void testDeleteAllRecords() throws HaloDBException, IOException {
        String directory = TestUtils.getTestDirectory("TombstoneFileCleanUpTest", "testDeleteAllRecords");

        HaloDBOptions options = new HaloDBOptions();
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

    @Test
    public void testDeleteAndInsertRecords() throws IOException, HaloDBException {
        String directory = TestUtils.getTestDirectory("TombstoneFileCleanUpTest", "testDeleteAndInsertRecords");

        HaloDBOptions options = new HaloDBOptions();
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

    @Test
    public void testDeleteRecordsWithoutCompaction() throws IOException, HaloDBException {
        String directory = TestUtils.getTestDirectory("TombstoneFileCleanUpTest", "testDeleteRecordsWithoutCompaction");

        HaloDBOptions options = new HaloDBOptions();
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

    @Test
    public void testWithCleanUpTurnedOff() throws IOException, HaloDBException {
        String directory = TestUtils.getTestDirectory("TombstoneFileCleanUpTest", "testWithCleanUpTurnedOff");

        HaloDBOptions options = new HaloDBOptions();
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

    @Test
    public void testCopyMultipleTombstoneFiles() throws HaloDBException, IOException {
        //Test to make sure that rollover to tombstone files work correctly during cleanup. 

        String directory = TestUtils.getTestDirectory("TombstoneFileCleanUpTest", "testCopyMultipleTombstoneFiles");

        HaloDBOptions options = new HaloDBOptions();
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

        // Since keyLength was 19 tombstone entry is 32 bytes.
        // Since file size is 512 there would be two tombstone files both of which should be copied.
        File[] tombstoneFiles = FileUtils.listTombstoneFiles(new File(directory));
        List<TombstoneEntry> tombstones = new ArrayList<>();
        Assert.assertEquals(tombstoneFiles.length, 2);
        for (File f : tombstoneFiles) {
            TombstoneFile tombstoneFile = new TombstoneFile(f, options, dbDirectory);
            tombstoneFile.open();
            TombstoneFile.TombstoneFileIterator iterator = tombstoneFile.newIterator();
            while (iterator.hasNext()) {
                tombstones.add(iterator.next());
            }
        }

        for (int i = 0; i < tombstones.size(); i++) {
            Assert.assertEquals(tombstones.get(i).getKey(), records.get(i).getKey());
        }

        HaloDBStats stats = db.stats();
        Assert.assertEquals(noOfRecords/2, stats.getNumberOfTombstonesFoundDuringOpen());
        Assert.assertEquals(0, stats.getNumberOfTombstonesCleanedUpDuringOpen());
    }
}
