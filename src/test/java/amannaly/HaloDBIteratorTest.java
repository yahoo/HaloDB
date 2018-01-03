package amannaly;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Arjun Mannaly
 */
public class HaloDBIteratorTest {

    //TODO: test with delete operation.

    @Test
    public void testPutAndGetDB() throws IOException {
        File directory = new File("/tmp/HaloDBIteratorTest/testPutAndGetDB");
        TestUtils.deleteDirectory(directory);

        HaloDBOptions options = new HaloDBOptions();
        options.isMergeDisabled = true;
        options.maxFileSize = 10*1024;

        HaloDB db = HaloDB.open(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

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
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        List<Record> updated = TestUtils.updateRecords(db, records);

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
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        List<Record> updated = TestUtils.updateRecords(db, records);

        // wait for merge to complete.
        Thread.sleep(10_000);

        List<Record> actual = new ArrayList<>();
        db.newIterator().forEachRemaining(actual::add);

        Assert.assertTrue(actual.containsAll(updated) && updated.containsAll(actual));

        db.close();
    }
}
