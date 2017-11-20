package amannaly;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Arjun Mannaly
 */
public class HaloDBDeletionTest extends TestBase {

    @Test
    public void testSimpleDelete() throws IOException {
        String directory = "/tmp/HaloDBDeletionTest/testSimpleDelete";
        HaloDBOptions options = new HaloDBOptions();
        options.isMergeDisabled = true;

        HaloDB db = testDB.getTestDB(directory, options);

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
                Assert.assertArrayEquals(records.get(i).getValue(), actual);
            }
        }
    }

    @Test
    public void testDeleteWithIterator() throws IOException {
        String directory = "/tmp/HaloDBDeletionTest/testDeleteWithIterator";
        HaloDBOptions options = new HaloDBOptions();
        options.isMergeDisabled = true;

        HaloDB db = testDB.getTestDB(directory, options);

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

    @Test
    public void testDeleteAndInsert() throws IOException {
        String directory = "/tmp/HaloDBDeletionTest/testDeleteAndInsert";
        HaloDBOptions options = new HaloDBOptions();
        options.isMergeDisabled = true;

        HaloDB db = testDB.getTestDB(directory, options);

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
                Assert.assertArrayEquals(records.get(i).getValue(), actual);
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
                Assert.assertArrayEquals(record.getValue(), value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // also check the iterator.
        List<Record> actual = new ArrayList<>();
        db.newIterator().forEachRemaining(actual::add);

        Assert.assertTrue(actual.containsAll(records) && records.containsAll(actual));
    }

    @Test
    public void testDeleteAndOpen() throws IOException {
        String directory = "/tmp/HaloDBDeletionTest/testDeleteAndOpen";
        HaloDBOptions options = new HaloDBOptions();
        options.isMergeDisabled = true;

        HaloDB db = testDB.getTestDB(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        // delete every other record.
        for (int i = 0; i < records.size(); i++) {
            if (i % 2 == 0) {
                db.delete(records.get(i).getKey());
            }
        }

        db.close();

        db = testDB.getTestDBWithoutDeletingFiles(directory, options);

        for (int i = 0; i < records.size(); i++) {
            byte[] actual = db.get(records.get(i).getKey());

            if (i % 2 == 0) {
                Assert.assertNull(actual);
            }
            else {
                Assert.assertArrayEquals(records.get(i).getValue(), actual);
            }
        }
    }

    @Test
    public void testDeleteAndMerge() throws Exception {
        String directory = "/tmp/HaloDBDeletionTest/testDeleteAndMerge";
        HaloDBOptions options = new HaloDBOptions();
        options.mergeJobIntervalInSeconds = 1;
        options.maxFileSize = 10 * 1024;
        options.mergeThresholdFileNumber = 1;
        options.mergeThresholdPerFile = 0.50;

        HaloDB db = testDB.getTestDB(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        // delete every other record.
        for (int i = 0; i < records.size(); i++) {
            if (i % 2 == 0) {
                db.delete(records.get(i).getKey());
            }
        }

        TestUtils.waitForMergeToComplete(db);

        db.close();

        db = testDB.getTestDBWithoutDeletingFiles(directory, options);

        for (int i = 0; i < records.size(); i++) {
            byte[] actual = db.get(records.get(i).getKey());

            if (i % 2 == 0) {
                Assert.assertNull(actual);
            }
            else {
                Assert.assertArrayEquals(records.get(i).getValue(), actual);
            }
        }
    }
}
