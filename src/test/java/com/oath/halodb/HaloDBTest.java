package com.oath.halodb;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Arjun Mannaly
 */
public class HaloDBTest extends TestBase {

    @Test
    public void testPutAndGetDB() throws HaloDBException {
        String directory = "/tmp/HaloDBTest/testPutAndGetDB";

        HaloDBOptions options = new HaloDBOptions();
        options.isCompactionDisabled = true;

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

    @Test
    public void testPutAndGetDBWithByteBuffer() throws HaloDBException {
        String directory = "/tmp/HaloDBTest/testPutAndGetDBWithByteBuffer";

        HaloDBOptions options = new HaloDBOptions();
        options.isCompactionDisabled = true;

        HaloDB db = getTestDB(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        List<Record> actual = new ArrayList<>();
        db.newIterator().forEachRemaining(actual::add);

        Assert.assertTrue(actual.containsAll(records) && records.containsAll(actual));

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        records.forEach(record -> {
            try {
                int read = db.get(record.getKey(), buffer);
                byte[] array = new byte[buffer.remaining()];
                buffer.get(array);
                Assert.assertEquals(record.getValue(), array);
                Assert.assertEquals(record.getValue().length, read);
            } catch (HaloDBException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testPutUpdateAndGetDB() throws HaloDBException {
        String directory = "/tmp/HaloDBTest/testPutUpdateAndGetDB";

        HaloDBOptions options = new HaloDBOptions();
        options.isCompactionDisabled = true;
        options.maxFileSize = 10*1024;

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

    @Test
    public void testCreateCloseAndOpenDB() throws HaloDBException {

        String directory = "/tmp/HaloDBTest/testCreateCloseAndOpenDB";

        HaloDBOptions options = new HaloDBOptions();
        options.isCompactionDisabled = true;
        options.maxFileSize = 10*1024;

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

    @Test
    public void testToCheckThatLatestUpdateIsPickedAfterDBOpen() throws HaloDBException {

        String directory = "/tmp/HaloDBTest/testToCheckThatLatestUpdateIsPickedAfterDBOpen";

        HaloDBOptions options = new HaloDBOptions();
        options.isCompactionDisabled = true;

        // sized to ensure that there will be two files.
        options.maxFileSize = 1500;

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

    @Test
    public void testToCheckDelete() throws HaloDBException {
        String directory = "/tmp/HaloDBTest/testToCheckDelete";

        HaloDBOptions options = new HaloDBOptions();
        options.isCompactionDisabled = true;
        options.maxFileSize = 10*1024;

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

    @Test
    public void testDeleteCloseAndOpen() throws HaloDBException {
        String directory = "/tmp/HaloDBTest/testDeleteCloseAndOpen";

        HaloDBOptions options = new HaloDBOptions();
        options.isCompactionDisabled = true;
        options.maxFileSize = 10*1024;

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

    @Test
    public void testDeleteAndInsert() throws HaloDBException {
        String directory = "/tmp/HaloDBTest/testDeleteAndInsert";

        HaloDBOptions options = new HaloDBOptions();
        options.isCompactionDisabled = true;
        options.maxFileSize = 10*1024;

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

    @Test
    public void testDeleteInsertCloseAndOpen() throws HaloDBException {
        String directory = "/tmp/HaloDBTest/testDeleteInsertCloseAndOpen";

        HaloDBOptions options = new HaloDBOptions();
        options.isCompactionDisabled = true;
        options.maxFileSize = 10*1024;

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
        String directory = Paths.get("tmp", "HaloDBTest", "testDBClose").toString();

        HaloDB db = getTestDB(directory, new HaloDBOptions());

        // Make sure that the META file was written.
        Assert.assertTrue(Paths.get(directory, DBMetaData.METADATA_FILE_NAME).toFile().exists());

        DBMetaData metaData = new DBMetaData(directory);
        metaData.loadFromFileIfExists();

        // Make sure that the open flag was set on db open.
        Assert.assertTrue(metaData.isOpen());

        // Default value of ioError flag must be false. 
        Assert.assertFalse(metaData.isIOError());

        db.close();

        // Make sure that the META file was written.
        Assert.assertTrue(Paths.get(directory, DBMetaData.METADATA_FILE_NAME).toFile().exists());

        // Make sure that the flags were set correctly on close.
        metaData.loadFromFileIfExists();

        Assert.assertFalse(metaData.isOpen());
        Assert.assertFalse(metaData.isIOError());
    }

    @Test(expectedExceptions = HaloDBException.class, expectedExceptionsMessageRegExp = "Another process already holds a lock for this db.")
    public void testLock() throws Throwable {
        String directory = Paths.get("tmp", "HaloDBTest", "testLock").toString();

        HaloDB db = getTestDB(directory, new HaloDBOptions());
        db.resetStats();
        try {
            HaloDB anotherDB = HaloDB.open(directory, new HaloDBOptions());
            anotherDB.resetStats();
        } catch (HaloDBException e) {
            throw e.getCause();
        }
    }
}
