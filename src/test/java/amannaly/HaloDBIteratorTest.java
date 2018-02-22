package amannaly;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * @author Arjun Mannaly
 */
public class HaloDBIteratorTest extends TestBase {

    @Test(expectedExceptions = NoSuchElementException.class)
    public void testWithEmptyDB() throws IOException {
        String directory = Paths.get("tmp", "HaloDBIteratorTest", "testWithEmptyDB").toString();

        HaloDB db = getTestDB(directory, new HaloDBOptions());
        HaloDB.HaloDBIterator iterator = db.newIterator();
        Assert.assertFalse(iterator.hasNext());
        iterator.next();
    }

    @Test
    public void testWithDelete() throws IOException {
        String directory = Paths.get("tmp", "HaloDBIteratorTest", "testWithEmptyDB").toString();

        HaloDBOptions options = new HaloDBOptions();
        options.isMergeDisabled = true;

        HaloDB db = getTestDB(directory, options);
        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        // delete all records.
        for (Record r : records) {
            db.delete(r.getKey());
        }

        HaloDB.HaloDBIterator iterator = db.newIterator();
        Assert.assertFalse(iterator.hasNext());

        // close and open the db again. 
        db.close();
        db = getTestDBWithoutDeletingFiles(directory, options);
        iterator = db.newIterator();
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testPutAndGetDB() throws IOException {
        String directory = "/tmp/HaloDBIteratorTest/testPutAndGetDB";

        HaloDBOptions options = new HaloDBOptions();
        options.isMergeDisabled = true;
        options.maxFileSize = 10*1024;

        HaloDB db = getTestDB(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        List<Record> actual = new ArrayList<>();
        db.newIterator().forEachRemaining(actual::add);

        MatcherAssert.assertThat(actual, Matchers.containsInAnyOrder(records.toArray()));
    }

    @Test
    public void testPutUpdateAndGetDB() throws IOException {
        String directory = "/tmp/HaloDBIteratorTest/testPutUpdateAndGetDB";

        HaloDBOptions options = new HaloDBOptions();
        options.isMergeDisabled = true;
        options.maxFileSize = 10*1024;

        HaloDB db = getTestDB(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        List<Record> updated = TestUtils.updateRecords(db, records);

        List<Record> actual = new ArrayList<>();
        db.newIterator().forEachRemaining(actual::add);
        MatcherAssert.assertThat(actual, Matchers.containsInAnyOrder(updated.toArray()));
    }

    @Test
    public void testPutUpdateMergeAndGetDB() throws IOException, InterruptedException {
        String directory = "/tmp/HaloDBIteratorTest/testPutUpdateMergeAndGetDB";

        HaloDBOptions options = new HaloDBOptions();
        options.isMergeDisabled = false;
        options.maxFileSize = 10*1024;
        options.mergeJobIntervalInSeconds = 2;
        options.mergeThresholdFileNumber = 2;
        options.mergeThresholdPerFile = 0.50;

        HaloDB db = getTestDB(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        List<Record> updated = TestUtils.updateRecords(db, records);

        TestUtils.waitForMergeToComplete(db);

        List<Record> actual = new ArrayList<>();
        db.newIterator().forEachRemaining(actual::add);
        MatcherAssert.assertThat(actual, Matchers.containsInAnyOrder(updated.toArray()));
    }
}
