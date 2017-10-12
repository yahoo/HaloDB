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

import static amannaly.RandomUtils.generateRandomAsciiString;

public class HaloDBIteratorTest {

    @Test
    public void testPutAndGetDB() throws IOException {
        File directory = new File("/tmp/HaloDBIteratorTest/testPutAndGetDB");
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

        List<Record> actual = new ArrayList<>();
        db.newIterator().forEachRemaining(actual::add);

        Assert.assertTrue(actual.containsAll(records) && records.containsAll(actual));
    }

    @Test
    public void testPutUpdateAndGetDB() throws IOException {
        File directory = new File("/tmp/HaloDBIteratorTest/testPutUpdateAndGetDB");
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

        db.close();
    }

    @Test
    public void testPutUpdateMergeAndGetDB() throws IOException, InterruptedException {
        File directory = new File("/tmp/HaloDBIteratorTest/testPutUpdateMergeAndGetDB");
        TestUtils.deleteDirectory(directory);

        HaloDBOptions options = new HaloDBOptions();
        options.isMergeDisabled = false;
        options.maxFileSize = 10*1024;
        options.mergeJobIntervalInSeconds = 2;
        options.mergeThresholdFileNumber = 2;
        options.mergeThresholdPerFile = 0.50;

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

        records.forEach(record -> {
            try {
                ByteString value = ByteString.copyFromUtf8(generateRandomAsciiString());
                db.put(record.getKey(), value);
                updated.add(new Record(record.getKey(), value));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // wait for merge to complete.
        Thread.sleep(10_000);

        List<Record> actual = new ArrayList<>();
        db.newIterator().forEachRemaining(actual::add);

        Assert.assertTrue(actual.containsAll(updated) && updated.containsAll(actual));

        db.close();
    }
}
