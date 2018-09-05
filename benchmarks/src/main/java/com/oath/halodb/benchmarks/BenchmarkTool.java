/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */
package com.oath.halodb.benchmarks;

import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.RateLimiter;

import org.HdrHistogram.Histogram;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class BenchmarkTool {

    private static SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

    // adjust HaloDB number of records accordingly.
    private final static int numberOfRecords = 500_000_000;

    private static volatile boolean isReadComplete = false;

    private static final int numberOfReads = 640_000_000;
    private static final int numberOfReadThreads = 32;
    private static final int noOfReadsPerThread = numberOfReads / numberOfReadThreads; // 400 million.

    private static final int writeMBPerSecond = 20 * 1024 * 1024;
    private static final RateLimiter writeRateLimiter = RateLimiter.create(writeMBPerSecond);

    private static final int recordSize = 1024;

    private static final int seed = 100;
    private static final Random random = new Random(seed);

    private static RandomDataGenerator randomDataGenerator = new RandomDataGenerator(seed);

    public static void main(String[] args) throws Exception {
        String directoryName = args[0];
        String benchmarkType = args[1];
        Benchmarks benchmark = null;
        try {
            benchmark = Benchmarks.valueOf(benchmarkType);
        }
        catch (IllegalArgumentException e) {
            System.out.println("Benchmarks should be one of " + Arrays.toString(Benchmarks.values()));
            System.exit(1);
        }

        System.out.println("Running benchmark " + benchmark);

        File dir = new File(directoryName);

        // select different storage engines here. 
        final StorageEngine db = new HaloDBStorageEngine(dir, numberOfRecords);
        //final StorageEngine db = new RocksDBStorageEngine(dir, numberOfRecords);
        //final StorageEngine db = new KyotoStorageEngine(dir, numberOfRecords);

        db.open();
        System.out.println("Opened the database.");

        switch (benchmark) {
            case FILL_SEQUENCE: createDB(db, true);break;
            case FILL_RANDOM: createDB(db, false);break;
            case READ_RANDOM: readRandom(db, numberOfReadThreads);break;
            case RANDOM_UPDATE: update(db);break;
            case READ_AND_UPDATE: updateWithReads(db);
        }

        db.close();
    }

    private static void createDB(StorageEngine db, boolean isSequential) {
        long start = System.currentTimeMillis();
        byte[] value;
        long dataSize = 0;

        for (int i = 0; i < numberOfRecords; i++) {
            value = randomDataGenerator.getData(recordSize);
            dataSize += (long)value.length;

            byte[] key = isSequential ? longToBytes(i) : longToBytes(random.nextInt(numberOfRecords));
            db.put(key, value);

            if (i % 1_000_000 == 0) {
                System.out.printf("%s: Wrote %d records\n", DateFormat.getTimeInstance().format(new Date()), i);
            }
        }

        long end = System.currentTimeMillis();
        long time = (end - start) / 1000;
        System.out.println("Completed writing data in " + time);
        System.out.printf("Write rate %d MB/sec\n", dataSize / time / 1024l / 1024l);
        System.out.println("Size of database " + db.size());
    }

    private static void update(StorageEngine db) {
        long start = System.currentTimeMillis();
        byte[] value;
        long dataSize = 0;

        for (int i = 0; i < numberOfRecords; i++) {
            value = randomDataGenerator.getData(recordSize);
            writeRateLimiter.acquire(value.length);
            dataSize += (long)value.length;

            byte[] key = longToBytes(random.nextInt(numberOfRecords));
            db.put(key, value);

            if (i % 1_000_000 == 0) {
                System.out.printf("%s: Wrote %d records\n", DateFormat.getTimeInstance().format(new Date()), i);
            }
        }

        long end = System.currentTimeMillis();
        long time = (end - start) / 1000;
        System.out.println("Completed over writing data in " + time);
        System.out.printf("Write rate %d MB/sec\n", dataSize / time / 1024l / 1024l);
        System.out.println("Size of database " + db.size());
    }

    private static void readRandom(StorageEngine db, int threads) {
        Read[] reads = new Read[numberOfReadThreads];

        long start = System.currentTimeMillis();
        for (int i = 0; i < reads.length; i++) {
            reads[i] = new Read(db, i);
            reads[i].start();
        }

        for (Read r : reads) {
            try {
                r.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        long time = (System.currentTimeMillis() - start) / 1000;

        System.out.printf("Completed %d reads with %d threads in %d seconds\n", numberOfReads, numberOfReadThreads, time);
        System.out.println("Operations per second - " + numberOfReads/time);

        Histogram latencyHistogram = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);
        for(Read r : reads) {
            latencyHistogram.add(r.latencyHistogram);
        }

        System.out.printf("Max value - %d\n", latencyHistogram.getMaxValue());
        System.out.printf("Average value - %f\n", latencyHistogram.getMean());
        System.out.printf("95th percentile - %d\n", latencyHistogram.getValueAtPercentile(95.0));
        System.out.printf("99th percentile - %d\n", latencyHistogram.getValueAtPercentile(99.0));
        System.out.printf("99.9th percentile - %d\n", latencyHistogram.getValueAtPercentile(99.9));
        System.out.printf("99.99th percentile - %d\n", latencyHistogram.getValueAtPercentile(99.99));
    }

    private static void updateWithReads(StorageEngine db) {
        Read[] reads = new Read[numberOfReadThreads];

        Thread update = new Thread(new Runnable() {
            @Override
            public void run() {
                long start = System.currentTimeMillis();
                byte[] value;
                long dataSize = 0, count = 0;

                while (!isReadComplete) {
                    value = randomDataGenerator.getData(recordSize);
                    writeRateLimiter.acquire(value.length);
                    dataSize += (long)value.length;

                    byte[] key = longToBytes(random.nextInt(numberOfRecords));
                    db.put(key, value);

                    if (count++ % 1_000_000 == 0) {
                        System.out.printf("%s: Wrote %d records\n", DateFormat.getTimeInstance().format(new Date()), count);
                    }
                }

                long end = System.currentTimeMillis();
                long time = (end - start) / 1000;
                System.out.println("Completed over writing data in " + time);
                System.out.println("Write operations per second - " + count/time);
                System.out.printf("Write rate %d MB/sec\n", dataSize / time / 1024l / 1024l);
                System.out.println("Size of database " + db.size());
            }
        });

        long start = System.currentTimeMillis();
        for (int i = 0; i < reads.length; i++) {
            reads[i] = new Read(db, i);
            reads[i].start();
        }

        update.start();

        for(Read r : reads) {
            try {
                r.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        long time = (System.currentTimeMillis() - start) / 1000;

        isReadComplete = true;

        long maxTime = -1;
        for (Read r : reads) {
            maxTime = Math.max(maxTime, r.time);
        }
        maxTime = maxTime / 1000;

        System.out.println("Maximum time taken by a read thread to complete - " + maxTime);

        System.out.printf("Completed %d reads with %d threads in %d seconds\n", numberOfReads, numberOfReadThreads, time);
        System.out.println("Read operations per second - " + numberOfReads/time);

        Histogram latencyHistogram = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);
        for(Read r : reads) {
            latencyHistogram.add(r.latencyHistogram);
        }

        System.out.printf("Max value - %d\n", latencyHistogram.getMaxValue());
        System.out.printf("Average value - %f\n", latencyHistogram.getMean());
        System.out.printf("95th percentile - %d\n", latencyHistogram.getValueAtPercentile(95.0));
        System.out.printf("99th percentile - %d\n", latencyHistogram.getValueAtPercentile(99.0));
        System.out.printf("99.9th percentile - %d\n", latencyHistogram.getValueAtPercentile(99.9));
        System.out.printf("99.99th percentile - %d\n", latencyHistogram.getValueAtPercentile(99.99));
    }


    static class Read extends Thread {
        final int id;
        final Random rand;
        final StorageEngine db;
        long time;

        Histogram latencyHistogram = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);

        Read(StorageEngine db, int id) {
            this.db = db;
            this.id = id;
            rand = new Random(seed + id);
        }

        @Override
        public void run() {
            long sum = 0, count = 0;
            long start = System.currentTimeMillis();

            while (count < noOfReadsPerThread) {
                long id = (long)rand.nextInt(numberOfRecords);
                long s = System.nanoTime();
                byte[] value = db.get(longToBytes(id));
                latencyHistogram.recordValue(System.nanoTime()-s);
                count++;
                if (value == null) {
                    System.out.println("NO value for key " +id);
                    continue;
                }

                if (count % 1_000_000 == 0) {
                    System.out.printf(printDate() + "Read: %d Completed %d reads\n", this.id, count);
                }

                sum += value.length;
            }

            time = (System.currentTimeMillis() - start);

            System.out.printf("Read: %d Completed in time %d\n", id, time);
        }
    }

    public static byte[] longToBytes(long value) {
        return Longs.toByteArray(value);
    }

    public static String printDate() {
        return sdf.format(new Date()) + ": ";
    }


}
