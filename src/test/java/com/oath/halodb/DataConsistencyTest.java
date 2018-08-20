/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import com.google.common.primitives.Longs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * @author Arjun Mannaly
 */
public class DataConsistencyTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(DataConsistencyTest.class);

    private final Object lock = new Object();
    private volatile boolean insertionComplete;
    private volatile boolean updatesComplete;
    private volatile boolean foundNonMatchingValue;

    private static final int fixedKeySize = 16;
    private static final int maxValueSize = 100;

    private static final int noOfRecords = 100_000;
    private static final int noOfTransactions = 1_000_000;

    private ByteBuffer[] keys;

    private RandomDataGenerator randDataGenerator;
    private Random random = new Random();

    private HaloDB haloDB;

    @BeforeMethod
    public void init() {
        insertionComplete = false;
        updatesComplete = false;
        foundNonMatchingValue = false;
        keys = new ByteBuffer[noOfRecords];
        randDataGenerator = new RandomDataGenerator();
    }

    @Test(dataProvider = "Options")
    public void testConcurrentReadAndUpdates(HaloDBOptions options) throws HaloDBException, InterruptedException {

        String directory = TestUtils.getTestDirectory("DataConsistencyCheck", "testConcurrentReadAndUpdates");

        options.setMaxFileSize(10 * 1024);
        options.setCompactionThresholdPerFile(0.1);
        options.setFixedKeySize(fixedKeySize);

        options.setNumberOfRecords(2 * noOfRecords);

        haloDB = getTestDB(directory, options);
        DataConsistencyDB db = new DataConsistencyDB(haloDB, noOfRecords);

        Writer writer = new Writer(db);
        writer.start();

        synchronized (lock) {
            while (!insertionComplete) {
                lock.wait();
            }
        }

        long start = System.currentTimeMillis();
        Reader[] readers = new Reader[10];

        for (int i = 0; i < readers.length; i++) {
            readers[i] = new Reader(db);
            readers[i].start();
        }

        writer.join();
        long totalReads = 0, totalReadSize = 0;
        for (Reader reader : readers) {
            reader.join();
            totalReads += reader.readCount;
            totalReadSize += reader.readSize;
        }
        long time = (System.currentTimeMillis() - start)/1000;

        Assert.assertFalse(foundNonMatchingValue);
        Assert.assertTrue(db.checkSize());

        haloDB.close();

        logger.info("Iterating and checking ...");
        HaloDB openAgainDB = getTestDBWithoutDeletingFiles(directory, options);
        TestUtils.waitForCompactionToComplete(openAgainDB);
        Assert.assertTrue(db.iterateAndCheck(openAgainDB));

        logger.info("Completed {} updates", writer.updateCount);
        logger.info("Completed {} deletes", writer.deleteCount);
        logger.info("Completed {} reads", totalReads);
        logger.info("Reads per second {}. {} MB/second", totalReads/time, totalReadSize/1024/1024/time);
        logger.info("Writes per second {}. {} KB/second", noOfTransactions/time, writer.totalWriteSize/1024/time);
        logger.info("Compaction rate {} KB/second", haloDB.stats().getCompactionRateSinceBeginning()/1024);
    }

    class Writer extends Thread {

        DataConsistencyDB db;
        long updateCount = 0;
        long deleteCount = 0;
        volatile long totalWriteSize = 0;
        Set<Integer> deletedKeys = new HashSet<>(50_000);

        Writer(DataConsistencyDB db) {
            this.db = db;
            setPriority(MAX_PRIORITY);
        }

        @Override
        public void run() {
            Random random = new Random();

            try {
                for (int i = 0; i < noOfRecords; i++) {
                    try {
                        byte[] key = randDataGenerator.getData(getRandomKeyLength());
                        while (db.containsKey(key)) {
                            key = randDataGenerator.getData(getRandomKeyLength());
                        }

                        keys[i] = ByteBuffer.wrap(key);
                        // we need at least 8 bytes for the version.
                        int size = random.nextInt(maxValueSize) + 9;
                        db.put(i, keys[i], generateRandomValueWithVersion(updateCount, size));
                    } catch (HaloDBException e) {
                        throw new RuntimeException(e);
                    }
                }
            } finally {
                synchronized (lock) {
                    insertionComplete = true;
                    lock.notify();
                }
            }

            try {
                while (!foundNonMatchingValue && updateCount < noOfTransactions) {
                    int k = random.nextInt(noOfRecords);
                    int size = random.nextInt(maxValueSize) + 9;
                    updateCount++;
                    try {
                        if (updateCount % 2 == 0) {
                            db.delete(k, keys[k]);
                            deleteCount++;
                            deletedKeys.add(k);
                            if (deletedKeys.size() == 50_000) {
                                int keyToAdd = deletedKeys.iterator().next();
                                db.put(keyToAdd, keys[keyToAdd], generateRandomValueWithVersion(updateCount, size));
                                totalWriteSize += size;
                                deletedKeys.remove(keyToAdd);
                            }
                        }
                        else {
                            db.put(k, keys[k], generateRandomValueWithVersion(updateCount, size));
                            totalWriteSize += size;
                        }
                    } catch (HaloDBException e) {
                        throw new RuntimeException(e);
                    }

                    if (updateCount > 0 && updateCount % 500_000 == 0) {
                        logger.info("Completed {} updates", updateCount);
                    }

                }
            } finally {
                updatesComplete = true;
            }
        }
    }

    class Reader extends Thread {

        DataConsistencyDB db;
        volatile long readCount = 0;
        volatile long readSize = 0;

        Reader(DataConsistencyDB db) {
            this.db = db;
            setPriority(MIN_PRIORITY);
        }

        @Override
        public void run() {
            Random random = new Random();
            while (!updatesComplete) {
                int i = random.nextInt(noOfRecords);
                try {
                    int valueSize = db.compareValues(i, keys[i]);
                    readCount++;
                    if (valueSize == -1) {
                        foundNonMatchingValue = true;
                    }
                    Assert.assertNotEquals(valueSize, -1, "Values don't match for key " + i);
                    readSize += valueSize;
                } catch (HaloDBException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private int getRandomKeyLength() {
        return random.nextInt(fixedKeySize) + 1;
    }

    private byte[] generateRandomValueWithVersion(long version, int size) {
        byte[] value = randDataGenerator.getData(size);
        System.arraycopy(Longs.toByteArray(version), 0, value, size - 8, 8);
        return value;
    }

    static long getVersionFromValue(byte[] value) {
        byte[] v = new byte[8];
        System.arraycopy(value, value.length-8, v, 0, 8);
        return Longs.fromByteArray(v);
    }
}