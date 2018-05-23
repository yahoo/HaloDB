package com.oath.halodb;

import com.google.common.util.concurrent.RateLimiter;

import org.testng.Assert;
import org.testng.annotations.Test;

import sun.nio.ch.FileChannelImpl;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Paths;
import java.util.List;

import mockit.Expectations;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.VerificationsInOrder;

/**
 * @author Arjun Mannaly
 */
public class CompactionWithErrorsTest extends TestBase {

    @Test
    public void testCompactionWithIOException(@Mocked CompactionManager compactionManager) throws HaloDBException, InterruptedException, IOException {
        new Expectations(CompactionManager.class) {{
            // nothing mocked. call the real implementation.
            // this is used only for verifications later.
        }};

        // Throw an IOException during IO operation.
        // IOExceptions are usually not recoverable hence we don't restart the compaction thread.
        new MockUp<FileChannelImpl>() {
            private int callCount = 0;

            @Mock
            public long transferTo(Invocation invocation, long position, long count, WritableByteChannel target) throws IOException {
                if (++callCount == 3) {
                    throw new IOException("Throwing mock IOException form compaction thread.");
                }

                return invocation.proceed(position, count, target);
            }
        };

        String directory = Paths.get("tmp", "CompactionManagerTest", "testCompactionWithIOException").toString();

        HaloDBOptions options = new HaloDBOptions();
        options.maxFileSize = 10 * 1024;
        options.compactionThresholdPerFile = 0.5;

        HaloDB db = getTestDB(directory, options);
        int numberOfRecords = 20; // three files.

        List<Record> records = insertAndUpdate(db, numberOfRecords);

        // Wait for the the compaction thread to crash. 
        Thread.sleep(2000);

        new VerificationsInOrder() {{
            // only called once when db.open(). Since an IOException is thrown the thread is not restarted.
            compactionManager.startCompactionThread(); times = 1;
        }};

        DBMetaData dbMetaData = new DBMetaData(directory);
        dbMetaData.loadFromFileIfExists();

        // Since compaction thread crashed with an IOException IOError flag must be set.
        Assert.assertTrue(dbMetaData.isIOError());
    }

    @Test
    public void testCompactionWithException() throws HaloDBException, InterruptedException {

        new MockUp<RateLimiter>() {
            private int callCount = 0;

            @Mock
            public double acquire(int permits) {
                if (++callCount == 3) {
                    // throw an exception when copying the third record. 
                    throw new RuntimeException("Throwing mock exception form compaction thread.");
                }
                return 10;
            }
        };

        String directory = Paths.get("tmp", "CompactionManagerTest", "testCompactionWithException").toString();

        HaloDBOptions options = new HaloDBOptions();
        options.maxFileSize = 10 * 1024;
        options.compactionThresholdPerFile = 0.5;

        HaloDB db = getTestDB(directory, options);
        int numberOfRecords = 30; // three files.

        List<Record> records = insertAndUpdate(db, numberOfRecords);

        TestUtils.waitForCompactionToComplete(db);

        // An exception was thrown while copying a record in the compaction thread.
        // Make sure that all records are still correct. 
        Assert.assertEquals(db.size(), records.size());
        for (Record r : records) {
            Assert.assertEquals(db.get(r.getKey()), r.getValue());
        }

        db.close();
        db = getTestDBWithoutDeletingFiles(directory, options);

        // Make sure that everything is good after
        // we open the db again. Since compaction had failed
        // there would be two copies of the same record in two different files. 
        Assert.assertEquals(db.size(), records.size());
        for (Record r : records) {
            Assert.assertEquals(db.get(r.getKey()), r.getValue());
        }
    }

    @Test
    public void testRestartCompactionThreadAfterCrash(@Mocked CompactionManager compactionManager) throws HaloDBException, InterruptedException, IOException {

        new Expectations(CompactionManager.class) {{
            // nothing mocked. call the real implementation.
            // this is used only for verifications later.
        }};

        new MockUp<RateLimiter>() {
            private int callCount = 0;

            @Mock
            public double acquire(int permits) {
                if (++callCount == 3 || callCount == 8) {
                    // throw exceptions twice, each time compaction thread should crash and restart. 
                    throw new OutOfMemoryError("Throwing mock exception from compaction thread.");
                }
                return 10;
            }
        };

        String directory = Paths.get("tmp", "CompactionManagerTest", "testRestartCompactionThreadAfterCrash").toString();

        HaloDBOptions options = new HaloDBOptions();
        options.maxFileSize = 10 * 1024;
        options.compactionThresholdPerFile = 0.5;

        HaloDB db = getTestDB(directory, options);
        int numberOfRecords = 30; // three files, 10 record in each.

        List<Record> records = insertAndUpdate(db, numberOfRecords);

        TestUtils.waitForCompactionToComplete(db);

        // An exception was thrown while copying a record in the compaction thread.
        // Make sure that all records are still correct.
        Assert.assertEquals(db.size(), records.size());
        for (Record r : records) {
            Assert.assertEquals(db.get(r.getKey()), r.getValue());
        }

        db.close();
        db = getTestDBWithoutDeletingFiles(directory, options);

        // Make sure that everything is good after
        // we open the db again. Since compaction had failed
        // there would be two copies of the same record in two different files.
        Assert.assertEquals(db.size(), records.size());
        for (Record r : records) {
            Assert.assertEquals(db.get(r.getKey()), r.getValue());
        }

        new VerificationsInOrder() {{
            // called when db.open()
            compactionManager.startCompactionThread();

            // compaction thread should have crashed twice and each time it should have been restarted.  
            compactionManager.startCompactionThread();
            compactionManager.startCompactionThread();

            // called after db.close()
            compactionManager.stopCompactionThread();

            // called when db.open() the second time. 
            compactionManager.startCompactionThread();
        }};

        DBMetaData dbMetaData = new DBMetaData(directory);
        dbMetaData.loadFromFileIfExists();

        // Since compaction thread was restarted after it crashed IOError flag must not be set.
        Assert.assertFalse(dbMetaData.isIOError());
    }

    private List<Record> insertAndUpdate(HaloDB db, int numberOfRecords) throws HaloDBException {
        List<Record> records = TestUtils.insertRandomRecordsOfSize(db, numberOfRecords, 1024 - Record.Header.HEADER_SIZE);

        // Update first 5 records in each file.
        for (int i = 0; i < 5; i++) {
            byte[] value = TestUtils.generateRandomByteArray();
            db.put(records.get(i).getKey(), value);
            records.set(i, new Record(records.get(i).getKey(), value));
            db.put(records.get(i+10).getKey(), value);
            records.set(i+10, new Record(records.get(i+10).getKey(), value));
        }
        return records;
    }
}
