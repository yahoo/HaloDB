/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import com.google.common.primitives.Longs;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Paths;

import mockit.Mock;
import mockit.MockUp;

public class HaloDBTest extends TestBase {

    @Test(dataProvider = "Options")
    public void testPutAndGetDB(HaloDBOptions options) throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBTest", "testPutAndGetDB");

        options.setCompactionDisabled(true);

        HaloDB db = getTestDB(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        List<Record> actual = new ArrayList<>();
        db.newIterator().forEachRemaining(actual::add);

        Assert.assertTrue(actual.containsAll(records) && records.containsAll(actual));

        records.forEach(record -> {
            try {
                byte[] value = db.get(record.getKey());
                Assert.assertEquals(record.getValue(), value);
            } catch (HaloDBException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test(dataProvider = "Options")
    public void testPutUpdateAndGetDB(HaloDBOptions options) throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBTest", "testPutUpdateAndGetDB");

        options.setCompactionDisabled(true);
        options.setMaxFileSize(10 * 1024);

        HaloDB db = getTestDB(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        List<Record> updated = TestUtils.updateRecords(db, records);

        List<Record> actual = new ArrayList<>();
        db.newIterator().forEachRemaining(actual::add);

        Assert.assertTrue(actual.containsAll(updated) && updated.containsAll(actual));

        updated.forEach(record -> {
            try {
                byte[] value = db.get(record.getKey());
                Assert.assertEquals(record.getValue(), value);
            } catch (HaloDBException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test(dataProvider = "Options")
    public void testPutAndContainsDB(HaloDBOptions options) throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBTest", "testPutAndGetDB");

        options.setCompactionDisabled(true);

        HaloDB db = getTestDB(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecordsOfSize(db, noOfRecords, 128);

        List<byte[]> missingKeys = new ArrayList<>();
        for (int i = 0; i < noOfRecords; i++) {
            missingKeys.add(TestUtils.generateRandomByteArray(64));
        }

        List<Record> actual = new ArrayList<>();
        db.newIterator().forEachRemaining(actual::add);

        Assert.assertTrue(actual.containsAll(records) && records.containsAll(actual));

        records.forEach(record -> Assert.assertTrue(db.contains(record.getKey())));

        missingKeys.forEach(key -> Assert.assertFalse(db.contains(key)));
    }


    @Test(dataProvider = "Options")
    public void testCreateCloseAndOpenDB(HaloDBOptions options) throws HaloDBException {

        String directory = TestUtils.getTestDirectory("HaloDBTest", "testCreateCloseAndOpenDB");

        options.setCompactionDisabled(true);
        options.setMaxFileSize(10 * 1024);

        HaloDB db = getTestDB(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        // update half the records.
        for (int i = 0; i < records.size(); i++) {
            if (i % 2 == 0) {
                Record record = records.get(i);
                try {
                    byte[] value = TestUtils.generateRandomByteArray();
                    db.put(record.getKey(), value);
                    records.set(i, new Record(record.getKey(), value));
                } catch (HaloDBException e) {
                    throw new RuntimeException(e);
                }
            }

        }

        db.close();

        // open and read contents again.
        HaloDB openAgainDB = getTestDBWithoutDeletingFiles(directory, options);

        List<Record> actual = new ArrayList<>();
        openAgainDB.newIterator().forEachRemaining(actual::add);

        Assert.assertTrue(actual.containsAll(records) && records.containsAll(actual));

        records.forEach(record -> {
            try {
                byte[] value = openAgainDB.get(record.getKey());
                Assert.assertEquals(record.getValue(), value);
            } catch (HaloDBException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test(dataProvider = "Options")
    public void testSyncWrite(HaloDBOptions options) throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBTest", "testSyncWrite");

        options.setCompactionDisabled(true);
        options.setMaxFileSize(10 * 1024);
        options.enableSyncWrites(true);

        HaloDB db = getTestDB(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);
        List<Record> updated = TestUtils.updateRecords(db, records);

        List<Record> actual = new ArrayList<>();
        db.newIterator().forEachRemaining(actual::add);

        Assert.assertTrue(actual.containsAll(updated) && updated.containsAll(actual));

        updated.forEach(record -> {
            try {
                byte[] value = db.get(record.getKey());
                Assert.assertEquals(record.getValue(), value);
            } catch (HaloDBException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test(dataProvider = "Options")
    public void testToCheckThatLatestUpdateIsPickedAfterDBOpen(HaloDBOptions options) throws HaloDBException {

        String directory = TestUtils.getTestDirectory("HaloDBTest", "testToCheckThatLatestUpdateIsPickedAfterDBOpen");

        options.setCompactionDisabled(true);

        // sized to ensure that there will be two files.
        options.setMaxFileSize(1500);

        HaloDB db = getTestDB(directory, options);

        byte[] key = TestUtils.generateRandomByteArray(7);
        byte[] value = null;

        // update the same record 100 times.
        // each key-value pair with the metadata is 20 bytes.
        // therefore 20 * 100 = 2000 bytes
        for (int i = 0; i < 100; i++) {
            value = TestUtils.generateRandomByteArray(7);
            db.put(key, value);
        }

        db.close();

        // open and read contents again.
        HaloDB openAgainDB = getTestDBWithoutDeletingFiles(directory, options);

        List<Record> actual = new ArrayList<>();
        openAgainDB.newIterator().forEachRemaining(actual::add);

        Assert.assertTrue(actual.size() == 1);

        Assert.assertEquals(openAgainDB.get(key), value);
    }

    @Test(dataProvider = "Options")
    public void testToCheckDelete(HaloDBOptions options) throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBTest", "testToCheckDelete");

        options.setCompactionDisabled(true);
        options.setMaxFileSize(10 * 1024);

        HaloDB db = getTestDB(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        List<Record> deleted = new ArrayList<>();
        for (int i = 0; i < noOfRecords; i++) {
            if (i % 10 == 0) deleted.add(records.get(i));
        }

        TestUtils.deleteRecords(db, deleted);

        List<Record> remaining = new ArrayList<>();
        db.newIterator().forEachRemaining(remaining::add);

        Assert.assertTrue(remaining.size() == noOfRecords - deleted.size());

        deleted.forEach(r -> {
            try {
                Assert.assertNull(db.get(r.getKey()));
            } catch (HaloDBException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test(dataProvider = "Options")
    public void testDeleteCloseAndOpen(HaloDBOptions options) throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBTest", "testDeleteCloseAndOpen");

        options.setCompactionDisabled(true);
        options.setMaxFileSize(10 * 1024);

        HaloDB db = getTestDB(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        List<Record> deleted = new ArrayList<>();
        for (int i = 0; i < noOfRecords; i++) {
            if (i % 10 == 0) deleted.add(records.get(i));
        }

        TestUtils.deleteRecords(db, deleted);

        db.close();

        HaloDB openAgainDB = getTestDBWithoutDeletingFiles(directory, options);

        List<Record> remaining = new ArrayList<>();
        openAgainDB.newIterator().forEachRemaining(remaining::add);

        Assert.assertTrue(remaining.size() == noOfRecords - deleted.size());

        deleted.forEach(r -> {
            try {
                Assert.assertNull(openAgainDB.get(r.getKey()));
            } catch (HaloDBException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test(dataProvider = "Options")
    public void testDeleteAndInsert(HaloDBOptions options) throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBTest", "testDeleteAndInsert");

        options.setCompactionDisabled(true);
        options.setMaxFileSize(10 * 1024);

        HaloDB db = getTestDB(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        List<Record> deleted = new ArrayList<>();
        for (int i = 0; i < noOfRecords; i++) {
            if (i % 10 == 0) deleted.add(records.get(i));
        }

        TestUtils.deleteRecords(db, deleted);

        List<Record> deleteAndInsert = new ArrayList<>();
        deleted.forEach(r -> {
            try {
                byte[] value = TestUtils.generateRandomByteArray();
                db.put(r.getKey(), value);
                deleteAndInsert.add(new Record(r.getKey(), value));
            } catch (HaloDBException e) {
                throw new RuntimeException(e);
            }
        });


        List<Record> remaining = new ArrayList<>();
        db.newIterator().forEachRemaining(remaining::add);

        Assert.assertTrue(remaining.size() == noOfRecords);

        deleteAndInsert.forEach(r -> {
            try {
                Assert.assertEquals(r.getValue(), db.get(r.getKey()));
            } catch (HaloDBException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test(dataProvider = "Options")
    public void testDeleteInsertCloseAndOpen(HaloDBOptions options) throws HaloDBException {
        String directory = TestUtils.getTestDirectory("tmp", "testDeleteInsertCloseAndOpen");

        options.setCompactionDisabled(true);
        options.setMaxFileSize(10 * 1024);

        HaloDB db = getTestDB(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        List<Record> deleted = new ArrayList<>();
        for (int i = 0; i < noOfRecords; i++) {
            if (i % 10 == 0) deleted.add(records.get(i));
        }

        TestUtils.deleteRecords(db, deleted);

        // inserting deleted records again. 
        List<Record> deleteAndInsert = new ArrayList<>();
        deleted.forEach(r -> {
            try {
                byte[] value = TestUtils.generateRandomByteArray();
                db.put(r.getKey(), value);
                deleteAndInsert.add(new Record(r.getKey(), value));
            } catch (HaloDBException e) {
                throw new RuntimeException(e);
            }
        });

        db.close();
        HaloDB openAgainDB = getTestDBWithoutDeletingFiles(directory, options);

        List<Record> remaining = new ArrayList<>();
        openAgainDB.newIterator().forEachRemaining(remaining::add);

        Assert.assertTrue(remaining.size() == noOfRecords);

        // make sure that records that were earlier deleted shows up now, since they were put back later.
        deleteAndInsert.forEach(r -> {
            try {
                Assert.assertEquals(r.getValue(), openAgainDB.get(r.getKey()));
            } catch (HaloDBException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testDBMetaFile() throws HaloDBException, IOException {
        String directory = TestUtils.getTestDirectory("HaloDBTest", "testDBMetaFile");

        HaloDBOptions options = new HaloDBOptions();
        int maxFileSize = 1024 * 1024 * 1024;
        options.setMaxFileSize(maxFileSize);
        HaloDB db = getTestDB(directory, options);

        // Make sure that the META file was written.
        Assert.assertTrue(Paths.get(directory, DBMetaData.METADATA_FILE_NAME).toFile().exists());

        DBMetaData metaData = new DBMetaData(dbDirectory);
        metaData.loadFromFileIfExists();

        // Make sure that the open flag was set on db open.
        Assert.assertTrue(metaData.isOpen());

        // Default value of ioError flag must be false. 
        Assert.assertFalse(metaData.isIOError());

        // since we just created the db max file size should be set to one we set in HaloDBOptions
        Assert.assertEquals(metaData.getMaxFileSize(), maxFileSize);
        Assert.assertEquals(metaData.getVersion(), Versions.CURRENT_META_FILE_VERSION);

        db.close();

        // Make sure that the META file was written.
        Assert.assertTrue(Paths.get(directory, DBMetaData.METADATA_FILE_NAME).toFile().exists());

        // Make sure that the flags were set correctly on close.
        metaData.loadFromFileIfExists();

        Assert.assertEquals(metaData.getVersion(), Versions.CURRENT_META_FILE_VERSION);
        Assert.assertFalse(metaData.isOpen());
        Assert.assertFalse(metaData.isIOError());
        Assert.assertEquals(metaData.getMaxFileSize(), maxFileSize);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "File size cannot be changed after db was created.*")
    public void testMaxFileSize() throws HaloDBException, IOException {
        String directory = TestUtils.getTestDirectory("HaloDBTest", "testMaxFileSize");

        HaloDBOptions options = new HaloDBOptions();
        int maxFileSize = 1024 * 1024 * 1024;
        options.setMaxFileSize(maxFileSize);
        HaloDB db = getTestDB(directory, options);

        DBMetaData metaData = new DBMetaData(dbDirectory);
        metaData.loadFromFileIfExists();

        Assert.assertEquals(metaData.getMaxFileSize(), maxFileSize);

        db.close();

        // try opening the db with max file size changed.
        options.setMaxFileSize(500 * 1024 * 1024);
        getTestDBWithoutDeletingFiles(directory, options);
    }

    @Test(expectedExceptions = HaloDBException.class, expectedExceptionsMessageRegExp = "Another process already holds a lock for this db.")
    public void testLock() throws Throwable {
        String directory = TestUtils.getTestDirectory("HaloDBTest", "testLock");

        HaloDB db = getTestDB(directory, new HaloDBOptions());
        db.resetStats();
        HaloDB anotherDB = HaloDB.open(directory, new HaloDBOptions());
        anotherDB.resetStats();
    }

    @Test
    public void testLockReleaseOnError() throws Throwable {

        new MockUp<DBMetaData>() {
            int count = 0;

            @Mock
            void loadFromFileIfExists() throws IOException {
                System.out.println("Mock called");
                if (count == 0) {
                    // throw an exception the first time the method is called.
                    count = 1;
                    throw new IOException();
                }
            }
        };

        String directory = TestUtils.getTestDirectory("HaloDBTest", "testLockReleaseOnError");

        HaloDB db = null;
        try {
            db = getTestDB(directory, new HaloDBOptions());
        } catch (HaloDBException e) {
            // swallow the mocked exception. 
        }
        // make sure open() failed.
        Assert.assertNull(db);

        // the lock should have been released when previous open() failed. 
        HaloDB anotherDB = getTestDBWithoutDeletingFiles(directory, new HaloDBOptions());
        anotherDB.put(Longs.toByteArray(1), Longs.toByteArray(1));
        Assert.assertEquals(anotherDB.size(), 1);
    }

    @Test(expectedExceptions = HaloDBException.class)
    public void testPutAfterClose() throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBTest", "testPutAfterClose");
        HaloDB db = getTestDB(directory, new HaloDBOptions());
        db.put(Longs.toByteArray(1), Longs.toByteArray(1));
        db.close();
        db.put(Longs.toByteArray(2), Longs.toByteArray(2));
    }

    @Test(expectedExceptions = HaloDBException.class)
    public void testDeleteAfterClose() throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBTest", "testDeleteAfterClose");

        HaloDB db = getTestDB(directory, new HaloDBOptions());
        db.put(Longs.toByteArray(1), Longs.toByteArray(1));
        db.put(Longs.toByteArray(2), Longs.toByteArray(2));
        db.delete(Longs.toByteArray(1));
        db.close();
        db.delete(Longs.toByteArray(2));
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testPutAfterCloseWithoutWrites() throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBTest", "testPutAfterCloseWithoutWrites");
        HaloDB db = getTestDB(directory, new HaloDBOptions());
        db.close();
        db.put(Longs.toByteArray(1), Longs.toByteArray(1));
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testDeleteAfterCloseWithoutWrites() throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBTest", "testDeleteAfterCloseWithoutWrites");

        HaloDB db = getTestDB(directory, new HaloDBOptions());
        db.put(Longs.toByteArray(1), Longs.toByteArray(1));
        Assert.assertEquals(db.get(Longs.toByteArray(1)), Longs.toByteArray(1));
        db.close();
        db.delete(Longs.toByteArray(1));
    }

    @Test
    public void testSnapshot() throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBTest", "testSnapshot");
        HaloDBOptions options = new HaloDBOptions();
        // Generate several data files
        options.setMaxFileSize(10000);
        HaloDB db = getTestDB(directory, options);
        for (int i = 10000; i < 10000 + 10*1000; i++) {
            db.put(ByteBuffer.allocate(4).putInt(i).array(), ByteBuffer.allocate(8).putInt(i).putInt(i*1024).array());
        }
        db.snapshot();

        String snapshotDir = db.getSnapshotDirectory().toString();
        HaloDB snapshotDB = getTestDBWithoutDeletingFiles(snapshotDir, options);

        for (int i = 10000; i < 10000 + 10*1000; i++) {
            byte[] value = snapshotDB.get(ByteBuffer.allocate(4).putInt(i).array());
            Assert.assertTrue(Arrays.equals(value, ByteBuffer.allocate(8).putInt(i).putInt(i*1024).array()));
        }

        snapshotDB.close();
        db.close();
    }

    @Test
    public void testCreateAndDeleteSnapshot() throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBTest", "testCreateAndDeleteSnapshot");
        HaloDBOptions options = new HaloDBOptions();
        // Generate several data files
        options.setMaxFileSize(10000);
        HaloDB db = getTestDB(directory, options);
        for (int i = 10000; i < 10000 + 10*1000; i++) {
            db.put(ByteBuffer.allocate(4).putInt(i).array(), ByteBuffer.allocate(8).putInt(i).putInt(i*1024).array());
        }

        Assert.assertTrue(db.clearSnapshot());
        db.snapshot();

        Assert.assertTrue(db.clearSnapshot());

        File snapshotDir = db.getSnapshotDirectory();
        Assert.assertFalse(snapshotDir.exists());

        db.close();
    }

    @Test
    public void testSnapshotAfterBeenCompacted() throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBTest", "testSnapshotAfterBeenCompacted");
        HaloDBOptions options = new HaloDBOptions();
        // Generate several data files
        options.setMaxFileSize(10000);
        options.setCompactionThresholdPerFile(0.7);
        HaloDB db = getTestDB(directory, options);
        for (int i = 10000; i < 10000 + 10*1000; i++) {
            db.put(ByteBuffer.allocate(4).putInt(i).array(), ByteBuffer.allocate(8).putInt(i).putInt(i*1024).array());
        }
        db.snapshot();

        // overwrite all previous record
        for (int i = 10000; i < 10000 + 10*1000; i++) {
            db.put(ByteBuffer.allocate(4).putInt(i).array(), ByteBuffer.allocate(8).putInt(i).putInt(i*2048).array());
        }
        TestUtils.waitForCompactionToComplete(db);

        String snapshotDir = db.getSnapshotDirectory().toString();
        HaloDB snapshotDB = getTestDBWithoutDeletingFiles(snapshotDir, options);

        for (int i = 10000; i < 10000 + 10*1000; i++) {
            byte[] key = ByteBuffer.allocate(4).putInt(i).array();
            byte[] value = snapshotDB.get(key);
            Assert.assertTrue(Arrays.equals(value, ByteBuffer.allocate(8).putInt(i).putInt(i*1024).array()));
            Assert.assertFalse(Arrays.equals(value, db.get(key)));
        }

        snapshotDB.close();
        db.close();
    }

}
