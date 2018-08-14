/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */
package com.oath.halodb.benchmarks;

import java.io.File;

import kyotocabinet.DB;

public class KyotoStorageEngine implements StorageEngine {

    private final File dbDirectory;
    private final int noOfRecords;

    private final DB db = new DB(2);

    public KyotoStorageEngine(File dbDirectory, int noOfRecords) {
        this.dbDirectory = dbDirectory;
        this.noOfRecords = noOfRecords;
    }

    @Override
    public void open() {
        int mode = DB.OWRITER | DB.OCREATE | DB.ONOREPAIR;
        StringBuilder fileNameBuilder = new StringBuilder();
        fileNameBuilder.append(dbDirectory.getPath()).append("/kyoto.kch");

        // specifies the power of the alignment of record size
        fileNameBuilder.append("#apow=").append(8);
        // specifies the number of buckets of the hash table
        fileNameBuilder.append("#bnum=").append(noOfRecords * 4);
        // specifies the mapped memory size
        fileNameBuilder.append("#msiz=").append(2_500_000_000l);
        // specifies the unit step number of auto defragmentation
        fileNameBuilder.append("#dfunit=").append(8);

        System.out.printf("Creating %s\n", fileNameBuilder.toString());

        if (!db.open(fileNameBuilder.toString(), mode)) {
            throw new IllegalArgumentException(String.format("KC db %s open error: " + db.error(),
                                                             fileNameBuilder.toString()));
        }
    }

    @Override
    public void put(byte[] key, byte[] value) {
        db.set(key, value);
    }

    @Override
    public byte[] get(byte[] key) {
        return db.get(key);
    }

    @Override
    public void close() {
        db.close();
    }

    @Override
    public long size() {
        return db.size();
    }
}
