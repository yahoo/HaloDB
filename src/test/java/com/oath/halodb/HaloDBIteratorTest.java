/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;

/**
 * @author Arjun Mannaly
 */
public class HaloDBIteratorTest extends TestBase {

    @Test(expectedExceptions = NoSuchElementException.class, dataProvider = "Options")
    public void testWithEmptyDB(HaloDBOptions options) throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBIteratorTest", "testWithEmptyDB");

        HaloDB db = getTestDB(directory, options);
        HaloDBIterator iterator = db.newIterator();
        Assert.assertFalse(iterator.hasNext());
        iterator.next();
    }

    @Test(dataProvider = "Options")
    public void testWithDelete(HaloDBOptions options) throws HaloDBException {
        String directory =  TestUtils.getTestDirectory("HaloDBIteratorTest", "testWithEmptyDB");

        options.setCompactionDisabled(true);

        HaloDB db = getTestDB(directory, options);
        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        // delete all records.
        for (Record r : records) {
            db.delete(r.getKey());
        }

        HaloDBIterator iterator = db.newIterator();
        Assert.assertFalse(iterator.hasNext());

        // close and open the db again. 
        db.close();
        db = getTestDBWithoutDeletingFiles(directory, options);
        iterator = db.newIterator();
        Assert.assertFalse(iterator.hasNext());
    }

    @Test(dataProvider = "Options")
    public void testPutAndGetDB(HaloDBOptions options) throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBIteratorTest", "testPutAndGetDB");

        options.setCompactionDisabled(true);
        options.setMaxFileSize(10 * 1024);

        HaloDB db = getTestDB(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        List<Record> actual = new ArrayList<>();
        db.newIterator().forEachRemaining(actual::add);

        MatcherAssert.assertThat(actual, Matchers.containsInAnyOrder(records.toArray()));
    }

    @Test(dataProvider = "Options")
    public void testPutUpdateAndGetDB(HaloDBOptions options) throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBIteratorTest", "testPutUpdateAndGetDB");

        options.setCompactionDisabled(true);
        options.setMaxFileSize(10 * 1024);

        HaloDB db = getTestDB(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        List<Record> updated = TestUtils.updateRecords(db, records);

        List<Record> actual = new ArrayList<>();
        db.newIterator().forEachRemaining(actual::add);
        MatcherAssert.assertThat(actual, Matchers.containsInAnyOrder(updated.toArray()));
    }

    @Test(dataProvider = "Options")
    public void testPutUpdateCompactAndGetDB(HaloDBOptions options) throws HaloDBException, InterruptedException {
        String directory = TestUtils.getTestDirectory("HaloDBIteratorTest", "testPutUpdateMergeAndGetDB");

        options.setCompactionDisabled(false);
        options.setMaxFileSize(10 * 1024);
        options.setCompactionThresholdPerFile(0.50);

        HaloDB db = getTestDB(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        List<Record> updated = TestUtils.updateRecords(db, records);

        TestUtils.waitForCompactionToComplete(db);

        List<Record> actual = new ArrayList<>();
        db.newIterator().forEachRemaining(actual::add);

        Assert.assertEquals(actual.size(), updated.size());
        MatcherAssert.assertThat(actual, Matchers.containsInAnyOrder(updated.toArray()));
    }

    // Test to make sure that no exceptions are thrown when files are being deleted by
    // compaction thread and db is being iterated. 
    @Test(dataProvider = "Options")
    public void testConcurrentCompactionAndIterator(HaloDBOptions options) throws HaloDBException, InterruptedException {
        String directory = TestUtils.getTestDirectory("HaloDBIteratorTest", "testConcurrentCompactionAndIterator");

        options.setMaxFileSize(1024 * 1024);
        options.setCompactionThresholdPerFile(0.1);

        final HaloDB db = getTestDB(directory, options);

        // insert 1024 records per file, and a total of 10 files.
        int noOfRecords = 10*1024;
        List<Record> records = TestUtils.insertRandomRecordsOfSize(db, noOfRecords, 1024-Record.Header.HEADER_SIZE);

        int noOfUpdateRuns = 10;
        Thread updateThread = new Thread(() -> {
            for (int i=0; i<noOfUpdateRuns; i++) {
                TestUtils.updateRecordsWithSize(db, records, 1024);
            }
        });
        // start updating the records. 
        updateThread.start();

        while (updateThread.isAlive()) {
            HaloDBIterator iterator = db.newIterator();
            while (iterator.hasNext()) {
                Assert.assertNotNull(iterator.next());
            }
        }
    }

    @Test(dataProvider = "Options")
    public void testConcurrentCompactionAndIteratorWhenFileIsClosed(HaloDBOptions options) throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBIteratorTest", "testConcurrentCompactionAndIterator");

        new MockUp<HaloDBFile>() {

            @Mock
            byte[] readFromFile(Invocation invocation, int offset, int length) throws HaloDBException {
                try {
                    // In the iterator after reading from keyCache pause for a while
                    // to increase the chance of file being closed by compaction thread.
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }

                return invocation.proceed(offset, length);
            }

        };

        options.setMaxFileSize(2 * 1024);
        options.setCompactionThresholdPerFile(0.1);

        final HaloDB db = getTestDB(directory, options);

        int noOfRecords = 4; // 2 records on 2 files. 
        List<Record> records = TestUtils.insertRandomRecordsOfSize(db, noOfRecords, 1024-Record.Header.HEADER_SIZE);

        int noOfUpdateRuns = 1000;
        Thread updateThread = new Thread(() -> {
            for (int i=0; i<noOfUpdateRuns; i++) {
                TestUtils.updateRecordsWithSize(db, records, 1024);
            }
        });
        // start updating the records.
        updateThread.start();

        while (updateThread.isAlive()) {
            HaloDBIterator iterator = db.newIterator();
            while (iterator.hasNext()) {
                Assert.assertNotNull(iterator.next());
            }
        }
    }

    @Test(dataProvider = "Options")
    public void testConcurrentCompactionAndIteratorWithMockedException(HaloDBOptions options) throws HaloDBException {
        // Previous tests are not guaranteed to throw ClosedChannelException. Here we throw a mock exception
        // to make sure that iterator gracefully handles files being closed and delete by compaction thread. 

        String directory = TestUtils.getTestDirectory("HaloDBIteratorTest", "testConcurrentCompactionAndIteratorWithMockedException");

        new MockUp<HaloDBFile>() {
            int count = 0;

            @Mock
            byte[] readFromFile(Invocation invocation, int offset, int length) throws IOException {
                if (count % 3 == 0) {
                    throw new ClosedChannelException();
                }
                return invocation.proceed(offset, length);
            }
        };

        options.setMaxFileSize(10 * 1024);
        options.setCompactionThresholdPerFile(0.6);

        final HaloDB db = getTestDB(directory, options);

        int noOfRecords = 50; // 50 records on 5 files.
        List<Record> records = TestUtils.insertRandomRecordsOfSize(db, noOfRecords, 1024-Record.Header.HEADER_SIZE);

        int noOfUpdateRuns = 100;
        Thread updateThread = new Thread(() -> {
            for (int i=0; i<noOfUpdateRuns; i++) {
                TestUtils.updateRecordsWithSize(db, records, 1024);
            }
        });
        // start updating the records.
        updateThread.start();

        while (updateThread.isAlive()) {
            HaloDBIterator iterator = db.newIterator();
            while (iterator.hasNext()) {
                Assert.assertNotNull(iterator.next());
            }
        }
    }
}
