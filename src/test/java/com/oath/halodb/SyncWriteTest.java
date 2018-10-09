package com.oath.halodb;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;

/**
 * @author Arjun Mannaly
 */
public class SyncWriteTest extends TestBase {

    @Test
    public void testSyncWrites() throws HaloDBException {
        String directory = TestUtils.getTestDirectory("SyncWriteTest", "testSyncWrites");

        AtomicInteger dataFileCount = new AtomicInteger(0);
        AtomicInteger tombstoneFileCount = new AtomicInteger(0);

        new MockUp<HaloDBFile>() {
            @Mock
            public void flushToDisk(Invocation invocation) throws IOException {
                dataFileCount.incrementAndGet();
            }
        };

        new MockUp<TombstoneFile>() {
            @Mock
            public void flushToDisk(Invocation invocation) throws IOException {
                tombstoneFileCount.incrementAndGet();
            }
        };

        HaloDBOptions options = new HaloDBOptions();
        options.enableSyncWrites(true);

        HaloDB db = getTestDB(directory, options);
        int noOfRecords = 10;
        List<Record> records = TestUtils.insertRandomRecords(db, noOfRecords);
        for (Record r : records) {
            db.delete(r.getKey());
        }

        // since sync write is enabled each record should have been flushed after write.
        Assert.assertEquals(dataFileCount.get(), noOfRecords);
        Assert.assertEquals(tombstoneFileCount.get(), noOfRecords);
    }

    @Test
    public void testNonSyncWrites() throws HaloDBException {
        String directory = TestUtils.getTestDirectory("SyncWriteTest", "testNonSyncWrites");

        AtomicInteger dataFileCount = new AtomicInteger(0);
        new MockUp<HaloDBFile>() {
            @Mock
            public void flushToDisk(Invocation invocation) throws IOException {
                dataFileCount.incrementAndGet();
            }
        };

        HaloDBOptions options = new HaloDBOptions();
        // value set to make sure that flush to disk will be called once.  
        options.setFlushDataSizeBytes(10 * 1024 - 1);

        HaloDB db = getTestDB(directory, options);
        int noOfRecords = 10;
        TestUtils.insertRandomRecordsOfSize(db, noOfRecords, 1024 - Record.Header.HEADER_SIZE);

        // 10 records of size 1024 each was inserted and flush size was set to 10 * 1024 - 1,
        // therefore data will be flushed to disk once. 
        Assert.assertEquals(dataFileCount.get(), 1);
    }

    @Test
    public void testNonSyncDeletes() throws HaloDBException {
        String directory = TestUtils.getTestDirectory("SyncWriteTest", "testNonSyncDeletes");

        AtomicInteger dataFileCount = new AtomicInteger(0);
        AtomicInteger tombstoneFileCount = new AtomicInteger(0);
        new MockUp<HaloDBFile>() {
            @Mock
            public void flushToDisk(Invocation invocation) throws IOException {
                dataFileCount.incrementAndGet();
            }
        };

        new MockUp<TombstoneFile>() {
            @Mock
            public void flushToDisk(Invocation invocation) throws IOException {
                tombstoneFileCount.incrementAndGet();
            }
        };

        HaloDBOptions options = new HaloDBOptions();
        // value set to make sure that flush to disk will not be called. 
        options.setFlushDataSizeBytes(1024 * 1024 * 1024);

        HaloDB db = getTestDB(directory, options);
        int noOfRecords = 100;
        List<Record> records = TestUtils.insertRandomRecordsOfSize(db, noOfRecords, 1024 - Record.Header.HEADER_SIZE);
        for (Record r : records) {
            db.delete(r.getKey());
        }

        // each record should have been flushed after write.
        Assert.assertEquals(dataFileCount.get(), 0);
        Assert.assertEquals(tombstoneFileCount.get(), 0);
    }

}
