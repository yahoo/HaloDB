/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */
package com.oath.halodb.benchmarks;

import com.google.common.primitives.Ints;
import com.oath.halodb.HaloDB;
import com.oath.halodb.HaloDBException;
import com.oath.halodb.HaloDBOptions;

import java.io.File;

public class HaloDBStorageEngine implements StorageEngine {

    private final File dbDirectory;

    private HaloDB db;
    private final long noOfRecords;

    public HaloDBStorageEngine(File dbDirectory, long noOfRecords) {
        this.dbDirectory = dbDirectory;
        this.noOfRecords = noOfRecords;
    }

    @Override
    public void put(byte[] key, byte[] value) {
        try {
            db.put(key, value);
        } catch (HaloDBException e) {
            e.printStackTrace();
        }

    }

    @Override
    public byte[] get(byte[] key) {
        try {
            return db.get(key);
        } catch (HaloDBException e) {
            e.printStackTrace();
        }

        return new byte[0];
    }

    @Override
    public void delete(byte[] key) {
        try {
            db.delete(key);
        } catch (HaloDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void open() {
        HaloDBOptions opts = new HaloDBOptions();
        opts.setMaxFileSize(1024*1024*1024);
        opts.setCompactionThresholdPerFile(0.50);
        opts.setFlushDataSizeBytes(10 * 1024 * 1024);
        opts.setNumberOfRecords(Ints.checkedCast(2 * noOfRecords));
        opts.setCompactionJobRate(135 * 1024 * 1024);
        opts.setUseMemoryPool(true);
        opts.setFixedKeySize(8);

        try {
            db = HaloDB.open(dbDirectory, opts);
        } catch (HaloDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        if (db != null){
            try {
                db.close();
            } catch (HaloDBException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public long size() {
        return db.size();
    }

    @Override
    public void printStats() {
        
    }

    @Override
    public String stats() {
        return db.stats().toString();
    }
}
