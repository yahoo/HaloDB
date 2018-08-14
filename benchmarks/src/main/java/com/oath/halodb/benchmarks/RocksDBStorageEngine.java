/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */
package com.oath.halodb.benchmarks;

import org.rocksdb.CompressionType;
import org.rocksdb.Env;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class RocksDBStorageEngine implements StorageEngine {

    private RocksDB db;
    private Options options;
    private Statistics statistics;
    private WriteOptions writeOptions;

    private final File dbDirectory;

    public RocksDBStorageEngine(File dbDirectory, int noOfRecords) {
        this.dbDirectory = dbDirectory;
    }

    @Override
    public void put(byte[] key, byte[] value) {
        try {
            db.put(writeOptions, key, value);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }

    }

    @Override
    public byte[] get(byte[] key) {
        byte[] value = null;
        try {
            value = db.get(key);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }

        return value;
    }

    @Override
    public void open() {
        options = new Options().setCreateIfMissing(true);
        options.setStatsDumpPeriodSec(1000000);

        options.setWriteBufferSize(128l * 1024 * 1024);
        options.setMaxWriteBufferNumber(3);
        options.setMaxBackgroundCompactions(20);

        Env env = Env.getDefault();
        env.setBackgroundThreads(20, Env.COMPACTION_POOL);
        options.setEnv(env);

        // max size of L1 10 MB.
        options.setMaxBytesForLevelBase(10485760);
        options.setTargetFileSizeBase(67108864);

        options.setLevel0FileNumCompactionTrigger(4);
        options.setLevel0SlowdownWritesTrigger(6);
        options.setLevel0StopWritesTrigger(12);
        options.setNumLevels(6);
        options.setDeleteObsoleteFilesPeriodMicros(300000000);


        options.setAllowMmapReads(false);
        options.setCompressionType(CompressionType.SNAPPY_COMPRESSION);


        System.out.printf("maxBackgroundCompactions %d \n", options.maxBackgroundCompactions());
        System.out.printf("minWriteBufferNumberToMerge %d \n", options.minWriteBufferNumberToMerge());
        System.out.printf("maxWriteBufferNumberToMaintain %d \n", options.maxWriteBufferNumberToMaintain());


        System.out.printf("level0FileNumCompactionTrigger %d \n", options.level0FileNumCompactionTrigger());
        System.out.printf("maxBytesForLevelBase %d \n", options.maxBytesForLevelBase());
        System.out.printf("maxBytesForLevelMultiplier %f \n", options.maxBytesForLevelMultiplier());
        System.out.printf("targetFileSizeBase %d \n", options.targetFileSizeBase());
        System.out.printf("targetFileSizeMultiplier %d \n", options.targetFileSizeMultiplier());

        List<CompressionType> compressionLevels =
            Arrays.asList(
                CompressionType.NO_COMPRESSION,
                CompressionType.NO_COMPRESSION,
                CompressionType.SNAPPY_COMPRESSION,
                CompressionType.SNAPPY_COMPRESSION,
                CompressionType.SNAPPY_COMPRESSION,
                CompressionType.SNAPPY_COMPRESSION
            );

        options.setCompressionPerLevel(compressionLevels);

        System.out.printf("compressionPerLevel %s \n", options.compressionPerLevel());
        System.out.printf("numLevels %s \n", options.numLevels());

        writeOptions = new WriteOptions();
        writeOptions.setDisableWAL(true);

        System.out.printf("WAL is disabled - %s \n", writeOptions.disableWAL());

        try {
            db = RocksDB.open(options, dbDirectory.getPath());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void close() {
        //statistics.close();
        options.close();
        writeOptions.close();
        db.close();
    }
}
