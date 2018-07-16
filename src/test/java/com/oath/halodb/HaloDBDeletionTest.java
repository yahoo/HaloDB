/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * @author Arjun Mannaly
 */
public class HaloDBDeletionTest extends TestBase {

    @Test(dataProvider = "Options")
    public void testSimpleDelete(HaloDBOptions options) throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBDeletionTest", "testSimpleDelete");
        options.setCompactionDisabled(true);

        HaloDB db = getTestDB(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        // delete every other record.
        for (int i = 0; i < records.size(); i++) {
            if (i % 2 == 0) {
                db.delete(records.get(i).getKey());
            }
        }

        for (int i = 0; i < records.size(); i++) {
            byte[] actual = db.get(records.get(i).getKey());

            if (i % 2 == 0) {
                Assert.assertNull(actual);
            }
            else {
                Assert.assertEquals(records.get(i).getValue(), actual);
            }
        }
    }

    @Test(dataProvider = "Options")
    public void testDeleteWithIterator(HaloDBOptions options) throws HaloDBException {
            String directory = TestUtils.getTestDirectory("HaloDBDeletionTest", "testDeleteWithIterator");
        options.setCompactionDisabled(true);

        HaloDB db = getTestDB(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        // delete every other record.
        List<Record> expected = new ArrayList<>();
        for (int i = 0; i < records.size(); i++) {
            if (i % 2 == 0) {
                db.delete(records.get(i).getKey());
            }
            else {
                expected.add(records.get(i));
            }
        }

        List<Record> actual = new ArrayList<>();
        db.newIterator().forEachRemaining(actual::add);

        Assert.assertTrue(actual.containsAll(expected) && expected.containsAll(actual));
    }

    @Test(dataProvider = "Options")
    public void testDeleteAndInsert(HaloDBOptions options) throws HaloDBException {
            String directory = TestUtils.getTestDirectory("HaloDBDeletionTest", "testDeleteAndInsert");
        options.setCompactionDisabled(true);

        HaloDB db = getTestDB(directory, options);

        int noOfRecords = 100;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        // delete every other record.
        for (int i = 0; i < records.size(); i++) {
            if (i % 2 == 0) {
                db.delete(records.get(i).getKey());
            }
        }

        for (int i = 0; i < records.size(); i++) {
            byte[] actual = db.get(records.get(i).getKey());

            if (i % 2 == 0) {
                Assert.assertNull(actual);
            }
            else {
                Assert.assertEquals(records.get(i).getValue(), actual);
            }
        }

        // insert deleted records.
        for (int i = 0; i < records.size(); i++) {
            if (i % 2 == 0) {
                byte[] value = TestUtils.generateRandomByteArray();
                byte[] key = records.get(i).getKey();
                db.put(key, value);
                records.set(i, new Record(key, value));
            }
        }

        records.forEach(record -> {
            try {
                byte[] value = db.get(record.getKey());
                Assert.assertEquals(record.getValue(), value);
            } catch (HaloDBException e) {
                throw new RuntimeException(e);
            }
        });

        // also check the iterator.
        List<Record> actual = new ArrayList<>();
        db.newIterator().forEachRemaining(actual::add);

        Assert.assertTrue(actual.containsAll(records) && records.containsAll(actual));
    }

    @Test(dataProvider = "Options")
    public void testDeleteAndOpen(HaloDBOptions options) throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBDeletionTest", "testDeleteAndOpen");
        options.setCompactionDisabled(true);

        HaloDB db = getTestDB(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        // delete every other record.
        for (int i = 0; i < records.size(); i++) {
            if (i % 2 == 0) {
                db.delete(records.get(i).getKey());
            }
        }

        db.close();

        db = getTestDBWithoutDeletingFiles(directory, options);

        for (int i = 0; i < records.size(); i++) {
            byte[] actual = db.get(records.get(i).getKey());

            if (i % 2 == 0) {
                Assert.assertNull(actual);
            }
            else {
                Assert.assertEquals(records.get(i).getValue(), actual);
            }
        }
    }

    @Test(dataProvider = "Options")
    public void testDeleteAndMerge(HaloDBOptions options) throws Exception {
        String directory = TestUtils.getTestDirectory("HaloDBDeletionTest", "testDeleteAndMerge");
        options.setMaxFileSize(10 * 1024);
        options.setCompactionThresholdPerFile(0.10);

        HaloDB db = getTestDB(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        // delete records
        Random random = new Random();
        Set<Integer> deleted = new HashSet<>();
        List<byte[]> newRecords = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            int index = random.nextInt(records.size());
            db.delete(records.get(index).getKey());
            deleted.add(index);

            // also throw in some new records into to mix.
            // size is 40 so that we create keys distinct from
            // what we used before.
            byte[] key = TestUtils.generateRandomByteArray(40);
            db.put(key, TestUtils.generateRandomByteArray());
            newRecords.add(key);
        }

        // update the new records to make sure the the files containing tombstones
        // will be compacted.
        for (byte[] key : newRecords) {
            db.put(key, TestUtils.generateRandomByteArray());
        }

        TestUtils.waitForCompactionToComplete(db);

        db.close();

        db = getTestDBWithoutDeletingFiles(directory, options);

        for (int i = 0; i < records.size(); i++) {
            byte[] actual = db.get(records.get(i).getKey());

            if (deleted.contains(i)) {
                Assert.assertNull(actual);
            }
            else {
                Assert.assertEquals(records.get(i).getValue(), actual);
            }
        }
    }

    @Test(dataProvider = "Options")
    public void testDeleteAllRecords(HaloDBOptions options) throws Exception {
        String directory = TestUtils.getTestDirectory("HaloDBDeletionTest", "testDeleteAllRecords");
        options.setMaxFileSize(10 * 1024);
        options.setCompactionThresholdPerFile(1);

        HaloDB db = getTestDB(directory, options);

        int noOfRecords = 10_000;
        // There will be 1000 files each of size 10KB
        List<Record> records = TestUtils.insertRandomRecordsOfSize(db, noOfRecords, 1024 - Record.Header.HEADER_SIZE);

        // delete all records.
        for (Record r : records) {
            db.delete(r.getKey());
        }

        TestUtils.waitForCompactionToComplete(db);

        Assert.assertEquals(db.size(), 0);

        for (Record r : records) {
            Assert.assertNull(db.get(r.getKey()));
        }

        // only the current write file will be remaining everything else should have been
        // deleted by the compaction job. 
        Assert.assertEquals(FileUtils.listDataFiles(new File(directory)).length, 1);
    }
}
