/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package example;

import com.google.common.primitives.Ints;
import com.oath.halodb.HaloDB;
import com.oath.halodb.HaloDBException;
import com.oath.halodb.HaloDBIterator;
import com.oath.halodb.HaloDBOptions;
import com.oath.halodb.HaloDBStats;
import com.oath.halodb.Record;

import java.io.File;
import java.io.IOException;

public class HaloDBExample {


    public void haloExample() {

        // Open a db with default options.
        HaloDBOptions options = new HaloDBOptions();
        // data file size will be 1GB.
        options.maxFileSize = 1024 * 1024 * 1024;

        // the threshold at which page cache is synced to disk.
        // data will be durable only if it is flushed to disk, therefore
        // more data will be lost if this value is set too high. Setting
        // this value too low might interfere with read performance.
        options.flushDataSizeBytes = 10 * 1024 * 1024;

        // The percentage of stale data in a data file at which the file will be compacted.
        // This value helps control write and space amplification. Increasing this value will
        // reduce write amplification but will increase space amplification.
        // This thus is the most important setting for tuning database performance.
        options.compactionThresholdPerFile = 0.75;

        // Controls how fact the compaction job should run.
        // This set the amount of data which will be copied by the compaction thread per second.
        options.compactionJobRate = 50 * 1024 * 1024;

        // Setting this value is important as it helps to preallocate enough
        // memory for the off-heap cache. If the value is too low the db might
        // need to rehash the cache.
        options.numberOfRecords = 100_000_000;


        // HaloDB represent a database instance and provides all methods for operating on the database.
        HaloDB db = null;

        // The directory will be created if it doesn't exist and all database files will be stored in this directory
        String directory = "directory";

        try {
            // Open the database. Directory will be created if it doesn't exist.
            // If we are opening an existing database HaloDB needs to scan all the
            // index files to create the in-memory cache, which, depending on the db size, might take a few minutes.
            db = HaloDB.open(directory, options);

            // key and values are byte arrays. Key size is restricted to 128 bytes.
            byte[] key1 = Ints.toByteArray(200);
            byte[] value1 = "Value for key 1".getBytes();

            byte[] key2 = Ints.toByteArray(300);
            byte[] value2 = "Value for key 2".getBytes();

            // add the key-value pair to the database.
            db.put(key1, value1);
            db.put(key2, value2);

            // read the value from the database.
            value1 = db.get(key1);
            value2 = db.get(value2);

            // delete a key from the database.
            db.delete(key1);

            // Open an iterator and iterate through all the key-value records.
            HaloDBIterator iterator = db.newIterator();
            while (iterator.hasNext()) {
                Record record = iterator.next();
                System.out.println(Ints.fromByteArray(record.getKey()));
                System.out.println(new String(record.getValue()));
            }

            // get stats and print it.
            HaloDBStats stats = db.stats();
            System.out.println(stats.toString());

            // reset stats
            db.resetStats();



        } catch (HaloDBException e) {
            e.printStackTrace();
        }

        if (db != null) {
            try {
                // Close the database.
                db.close();
            } catch (HaloDBException e) {
                e.printStackTrace();
            }
        }

    }
}
