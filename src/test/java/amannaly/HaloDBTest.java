package amannaly;

import com.google.protobuf.ByteString;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static amannaly.RandomUtils.*;

public class HaloDBTest {

    @Test
    public void testPutAndGetDB() throws IOException {
        File directory = new File("/tmp/HaloDBTest/testPutAndGetDB");
        TestUtils.deleteDirectory(directory);

        HaloDBOptions options = new HaloDBOptions();
        options.isMergeDisabled = true;

        HaloDB db = HaloDB.open(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = new ArrayList<>();
        Set<ByteString> keySet = new HashSet<>();

        for (int i = 0; i < noOfRecords; i++) {
            ByteString key = ByteString.copyFromUtf8(generateRandomAsciiString());
            ByteString value = ByteString.copyFromUtf8(generateRandomAsciiString());
            if (keySet.contains(key))
                continue;

            records.add(new Record(key, value));
            keySet.add(key);

            db.put(key, value);
        }

        List<Record> actual = new ArrayList<>();
        db.newIterator().forEachRemaining(actual::add);

        Assert.assertTrue(actual.containsAll(records) && records.containsAll(actual));

        records.forEach(record -> {
            try {
                ByteString value = db.get(record.getKey());
                Assert.assertEquals(record.getValue(), value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        db.close();
    }

    @Test
    public void testPutUpdateAndGetDB() throws IOException {
        File directory = new File("/tmp/HaloDBTest/testPutUpdateAndGetDB");
        TestUtils.deleteDirectory(directory);

        HaloDBOptions options = new HaloDBOptions();
        options.isMergeDisabled = true;
        options.maxFileSize = 10*1024;

        HaloDB db = HaloDB.open(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = new ArrayList<>();
        Set<ByteString> keySet = new HashSet<>();

        for (int i = 0; i < noOfRecords; i++) {
            ByteString key = ByteString.copyFromUtf8(generateRandomAsciiString());
            ByteString value = ByteString.copyFromUtf8(generateRandomAsciiString());
            if (keySet.contains(key))
                continue;

            records.add(new Record(key, value));
            keySet.add(key);

            db.put(key, value);
        }

        List<Record> updated = new ArrayList<>();

        // update all records.
        records.forEach(record -> {
            try {
                ByteString value = ByteString.copyFromUtf8(generateRandomAsciiString());
                db.put(record.getKey(), value);
                updated.add(new Record(record.getKey(), value));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        List<Record> actual = new ArrayList<>();
        db.newIterator().forEachRemaining(actual::add);

        Assert.assertTrue(actual.containsAll(updated) && updated.containsAll(actual));

        updated.forEach(record -> {
            try {
                ByteString value = db.get(record.getKey());
                Assert.assertEquals(record.getValue(), value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        db.close();
    }

    @Test
    public void testCreateCloseAndOpenDB() throws IOException {

        File directory = new File("/tmp/HaloDBTest/testCreateCloseAndOpenDB");
        TestUtils.deleteDirectory(directory);

        HaloDBOptions options = new HaloDBOptions();
        options.isMergeDisabled = true;
        options.maxFileSize = 10*1024;

        HaloDB db = HaloDB.open(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = new ArrayList<>();
        Set<ByteString> keySet = new HashSet<>();

        for (int i = 0; i < noOfRecords; i++) {
            ByteString key = ByteString.copyFromUtf8(generateRandomAsciiString());
            ByteString value = ByteString.copyFromUtf8(generateRandomAsciiString());
            if (keySet.contains(key))
                continue;

            records.add(new Record(key, value));
            keySet.add(key);

            db.put(key, value);
        }

        // update half the records.
        for (int i = 0; i < records.size(); i++) {
            if (i % 2 == 0) {
                Record record = records.get(i);
                try {
                    ByteString value = ByteString.copyFromUtf8(generateRandomAsciiString());
                    db.put(record.getKey(), value);
                    records.set(i, new Record(record.getKey(), value));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

        }

        db.close();


        // open and read contents again.
        HaloDB openAgainDB = HaloDB.open(directory, options);

        List<Record> actual = new ArrayList<>();
        openAgainDB.newIterator().forEachRemaining(actual::add);

        Assert.assertTrue(actual.containsAll(records) && records.containsAll(actual));

        records.forEach(record -> {
            try {
                ByteString value = openAgainDB.get(record.getKey());
                Assert.assertEquals(record.getValue(), value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        openAgainDB.close();
    }

    @Test
    public void testToCheckThatLatestUpdateIsPickedAfterDBOpen() throws IOException {

        File directory = new File("/tmp/HaloDBTest/testToCheckThatLatestUpdateIsPickedAfterDBOpen");
        TestUtils.deleteDirectory(directory);

        HaloDBOptions options = new HaloDBOptions();
        options.isMergeDisabled = true;

        // sized to ensure that there will be two files.
        options.maxFileSize = 1500;

        HaloDB db = HaloDB.open(directory, options);

        ByteString key = ByteString.copyFromUtf8(generateRandomAsciiString(7));
        ByteString value = null;

        // update the same record 100 times.
        // each key-value pair with the metadata is 20 bytes.
        // therefore 20 * 100 = 2000 bytes
        for (int i = 0; i < 100; i++) {
            value = ByteString.copyFromUtf8(generateRandomAsciiString(7));
            db.put(key, value);
        }

        db.close();

        // open and read contents again.
        HaloDB openAgainDB = HaloDB.open(directory, options);

        List<Record> actual = new ArrayList<>();
        openAgainDB.newIterator().forEachRemaining(actual::add);

        Assert.assertTrue(actual.size() == 1);

        Assert.assertEquals(openAgainDB.get(key), value);
        openAgainDB.close();
    }
}
