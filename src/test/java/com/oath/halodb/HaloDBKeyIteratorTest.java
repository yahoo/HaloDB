package com.oath.halodb;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.*;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HaloDBKeyIteratorTest extends TestBase {

    @Test(expectedExceptions = NoSuchElementException.class, dataProvider = "Options")
    public void testWithEmptyDB(HaloDBOptions options) throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBKeyIteratorTest", "testWithEmptyDB");

        HaloDB db = getTestDB(directory, options);
        HaloDBKeyIterator iterator = db.newKeyIterator();
        Assert.assertFalse(iterator.hasNext());
        iterator.next();
    }

    @Test(dataProvider = "Options")
    public void testWithDelete(HaloDBOptions options) throws HaloDBException {
        String directory =  TestUtils.getTestDirectory("HaloDBKeyIteratorTest", "testWithEmptyDB");

        options.setCompactionDisabled(true);

        HaloDB db = getTestDB(directory, options);
        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        // delete all records.
        for (Record r : records) {
            db.delete(r.getKey());
        }

        HaloDBKeyIterator iterator = db.newKeyIterator();
        Assert.assertFalse(iterator.hasNext());

        // close and open the db again.
        db.close();
        db = getTestDBWithoutDeletingFiles(directory, options);
        iterator = db.newKeyIterator();
        Assert.assertFalse(iterator.hasNext());
    }

    @Test(dataProvider = "Options")
    public void testPutAndGetDB(HaloDBOptions options) throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBKeyIteratorTest", "testPutAndGetDB");

        options.setCompactionDisabled(true);
        options.setMaxFileSize(10 * 1024);

        HaloDB db = getTestDB(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        List<RecordKey> keys = new LinkedList<>();
        for (Record record : records) {
            keys.add(new RecordKey(record.getKey()));
        }

        List<RecordKey> actual = new ArrayList<>();
        db.newKeyIterator().forEachRemaining(actual::add);

        MatcherAssert.assertThat(actual, Matchers.containsInAnyOrder(keys.toArray()));
    }

    @Test(dataProvider = "Options")
    public void testPutUpdateAndGetDB(HaloDBOptions options) throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBKeyIteratorTest", "testPutUpdateAndGetDB");

        options.setCompactionDisabled(true);
        options.setMaxFileSize(10 * 1024);

        HaloDB db = getTestDB(directory, options);

        int noOfRecords = 10_000;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);

        List<Record> updated = TestUtils.updateRecords(db, records);

        List<RecordKey> keys = new LinkedList<>();
        for (Record record : updated) {
            keys.add(new RecordKey(record.getKey()));
        }

        List<RecordKey> actual = new ArrayList<>();
        db.newKeyIterator().forEachRemaining(actual::add);
        MatcherAssert.assertThat(actual, Matchers.containsInAnyOrder(keys.toArray()));
    }
}
