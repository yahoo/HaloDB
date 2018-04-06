package com.oath.halodb;

import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Random;

/**
 * @author Arjun Mannaly
 */
public class DataConsistencyCheck extends TestBase {

    @Test
    public void testConcurrentReadAndUpdates() throws IOException, InterruptedException {
        String directory = TestUtils.getTestDirectory("DataConsistencyCheck", "testConcurrentReadAndUpdates");

        HaloDBOptions options = new HaloDBOptions();
        options.maxFileSize = 10 * 1024;
        options.compactionThresholdPerFile = 0.5;

        int noOfRecords = 10_000;
        int noOfTransactions = 100_000;

        HaloDB haloDB = getTestDB(directory, options);
        DataConsistencyDB db = new DataConsistencyDB(haloDB);

        Writer writer = new Writer(noOfRecords, noOfTransactions, db);
        writer.start();

        synchronized (lock) {
            while (!insertionComplete) {
                lock.wait();
            }
        }

        Reader[] readers = new Reader[10];

        for (int i = 0; i < readers.length; i++) {
            readers[i] = new Reader(noOfRecords, db);
            readers[i].start();
        }

        writer.join();
    }

    static final Object lock = new Object();
    static volatile boolean insertionComplete = false;
    static volatile boolean updatesComplete = false;

    static class Writer extends Thread {

        int noOfRecords;
        DataConsistencyDB db;
        int numberOfTransactions;

        Writer(int noOfRecords, int numberOfTransactions, DataConsistencyDB db) {
            this.noOfRecords = noOfRecords;
            this.db = db;
            this.numberOfTransactions = numberOfTransactions;
        }

        @Override
        public void run() {
            Random random = new Random();

            for (int i = 0; i < noOfRecords; i++) {
                try {
                    db.put(i, TestUtils.generateRandomByteArray());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            synchronized (lock) {
                insertionComplete = true;
                lock.notify();
            }

            for (long i = 0; i < numberOfTransactions; i++) {
                int k = random.nextInt(noOfRecords);
                try {
                    db.put(k, TestUtils.generateRandomByteArray());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            updatesComplete = true;
        }
    }

    static class Reader extends Thread {

        int noOfRecords;
        DataConsistencyDB db;

        Reader(int noOfRecords, DataConsistencyDB db) {
            this.noOfRecords = noOfRecords;
            this.db = db;
        }

        @Override
        public void run() {
            Random random = new Random();
            while (!updatesComplete) {
                int i = random.nextInt(noOfRecords);
                try {
                    db.get(i);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
