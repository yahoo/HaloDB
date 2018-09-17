/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Pulkit Goel
 */
public class SequenceNumberTest extends TestBase {

    @Test(dataProvider = "Options")
    public void testSequenceNumber(HaloDBOptions options) throws Exception {
        String directory = TestUtils.getTestDirectory("SequenceNumberTest", "testSequenceNumber");

        int totalNumberOfRecords = 10;
        options.setMaxFileSize(1024 * 1024 * 1024);

        // Write 10 records in the DB
        HaloDB db = getTestDB(directory, options);
        TestUtils.insertRandomRecords(db, totalNumberOfRecords);

        // Iterate through all records, atleast one record should have 10 as the sequenceNumber
        File file = Arrays.stream(FileUtils.listDataFiles(new File(directory))).max(Comparator.comparing(File::getName)).get();
        HaloDBFile.HaloDBFileIterator haloDBFileIterator = HaloDBFile.openForReading(dbDirectory, file, HaloDBFile.FileType.DATA_FILE, options).newIterator();

        List<Long> sequenceNumbers = new ArrayList<>();
        int count = 1;
        while (haloDBFileIterator.hasNext()) {
            Record record = haloDBFileIterator.next();
            sequenceNumbers.add(record.getSequenceNumber());
            Assert.assertEquals(record.getSequenceNumber(), count++);
        }
        Assert.assertTrue(sequenceNumbers.contains(10L));
        db.close();

        // open and read the content again
        HaloDB reopenedDb = getTestDBWithoutDeletingFiles(directory, options);
        List<Record> records = new ArrayList<>();
        reopenedDb.newIterator().forEachRemaining(records::add);

        // Verify that the sequence number is still present after reopening the DB
        sequenceNumbers = records.stream().map(record -> record.getRecordMetaData().getSequenceNumber()).collect(Collectors.toList());
        count = 1;
        for (long s : sequenceNumbers) {
            Assert.assertEquals(s, count++);
        }
        Assert.assertTrue(sequenceNumbers.contains(10L));

        // Write 10 records in the DB
        TestUtils.insertRandomRecords(reopenedDb, totalNumberOfRecords);

        // Iterate through all records, atleast one record should have 119 as the sequenceNumber (10 original records + 99 offset for reopening + 10 new records)
        file = Arrays.stream(FileUtils.listDataFiles(new File(directory))).max(Comparator.comparing(File::getName)).get();
        haloDBFileIterator = HaloDBFile.openForReading(dbDirectory, file, HaloDBFile.FileType.DATA_FILE, options).newIterator();

        sequenceNumbers = new ArrayList<>();
        count = 110;
        while (haloDBFileIterator.hasNext()) {
            Record record = haloDBFileIterator.next();
            sequenceNumbers.add(record.getSequenceNumber());
            Assert.assertEquals(record.getSequenceNumber(), count++);
        }
        Assert.assertTrue(sequenceNumbers.contains(119L));

        // Delete the first 10 records from the DB
        for (Record record : records) {
            reopenedDb.delete(record.getKey());
        }

        // get the tombstone file.
        File[] tombstoneFiles = FileUtils.listTombstoneFiles(new File(directory));
        Assert.assertEquals(tombstoneFiles.length, 1);

        TombstoneFile tombstoneFile = new TombstoneFile(tombstoneFiles[0], options, dbDirectory);
        tombstoneFile.open();
        List<TombstoneEntry> tombstoneEntries = new ArrayList<>();
        tombstoneFile.newIterator().forEachRemaining(tombstoneEntries::add);

        Assert.assertEquals(tombstoneEntries.size(), 10);
        count = 120;
        for (TombstoneEntry tombstoneEntry : tombstoneEntries) {
            // Each tombstoneEntry should have sequence number greater than or equal to 119 (10 original records + 99 offset for reopening + 10 new records)
            Assert.assertEquals(tombstoneEntry.getSequenceNumber(), count++);
        }
        reopenedDb.close();

        // reopen the db and add the content again
        reopenedDb = getTestDBWithoutDeletingFiles(directory, options);

        // Write 10 records in the DB
        TestUtils.insertRandomRecords(reopenedDb, totalNumberOfRecords);

        // Iterate through all records, atleast one record should have 238 as the sequenceNumber (10 original records + 99 offset for reopening + 10 records + 10 tombstone records + 99 offset for reopening + 10 new records)
        file = Arrays.stream(FileUtils.listDataFiles(new File(directory))).max(Comparator.comparing(File::getName)).get();
        haloDBFileIterator = HaloDBFile.openForReading(dbDirectory, file, HaloDBFile.FileType.DATA_FILE, options).newIterator();

        sequenceNumbers = new ArrayList<>();
        count = 229;
        while (haloDBFileIterator.hasNext()) {
            Record record = haloDBFileIterator.next();
            sequenceNumbers.add(record.getSequenceNumber());
            Assert.assertEquals(record.getSequenceNumber(), count++);
        }
        Assert.assertTrue(sequenceNumbers.contains(238L));
        reopenedDb.close();
    }
}
