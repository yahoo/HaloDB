/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Holds an instance of HaloDB and Java's ConcurrentHashMap.
 * Tests will use this to insert data into both and ensure that
 * the data in HaloDB is correct. 
 *
 * @author Arjun Mannaly
 */

class DataConsistencyDB {
    private static final Logger logger = LoggerFactory.getLogger(DataConsistencyDB.class);

    //TODO: allocate this off-heap.
    private final Map<ByteBuffer, byte[]> javaMap = new ConcurrentHashMap<>();
    private final HaloDB haloDB;

    private int numberOfLocks = 100;

    private final ReentrantReadWriteLock[] locks;

    DataConsistencyDB(HaloDB haloDB, int noOfRecords) {
        this.haloDB = haloDB;

        locks = new ReentrantReadWriteLock[numberOfLocks];
        for (int i = 0; i < numberOfLocks; i++) {
            locks[i] = new ReentrantReadWriteLock();
        }
    }

    void put(int keyIndex, ByteBuffer keyBuf, byte[] value) throws HaloDBException {
        ReentrantReadWriteLock lock = locks[keyIndex%numberOfLocks];
        try {
            lock.writeLock().lock();
            javaMap.put(keyBuf, value);
            haloDB.put(keyBuf.array(), value);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    // return -1 if values don't match.
    int compareValues(int keyIndex, ByteBuffer keyBuf) throws HaloDBException {
        ReentrantReadWriteLock lock = locks[keyIndex%numberOfLocks];
        try {
            lock.readLock().lock();
            return checkValues(keyIndex, keyBuf, haloDB);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    boolean checkSize() {
        return haloDB.size() == javaMap.size();
    }

    void delete(int keyIndex, ByteBuffer keyBuf) throws HaloDBException {
        ReentrantReadWriteLock lock = locks[keyIndex%numberOfLocks];
        try {
            lock.writeLock().lock();
            javaMap.remove(keyBuf);
            haloDB.delete(keyBuf.array());
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    boolean iterateAndCheck(HaloDB db) {
        if (db.size() != javaMap.size()) {
            logger.error("Size don't match {} != {}", db.size(), javaMap.size());
            return false;
        }

        for (Map.Entry<ByteBuffer, byte[]> entry : javaMap.entrySet()) {
            try {
                if (!Arrays.equals(entry.getValue(), db.get(entry.getKey().array()))) {
                    return false;
                }

            } catch (HaloDBException e) {
                logger.error("Error while iterating", e);
                return false;
            }
        }
        return true;
    }

    // return -1 if values don't match.
    private int checkValues(long key, ByteBuffer keyBuf, HaloDB haloDB) throws HaloDBException {
        byte[] mapValue = javaMap.get(keyBuf);
        byte[] dbValue = haloDB.get(keyBuf.array());
        if (Arrays.equals(mapValue, dbValue))
            return dbValue == null ? 0 : dbValue.length;

        if (mapValue == null) {
            logger.error("Map value is null for key {} of length {} but HaloDB value has version {}",
                         key, keyBuf.remaining(), DataConsistencyTest.getVersionFromValue(dbValue));
        }
        else if (dbValue == null) {
            logger.error("HaloDB value is null for key {} of length {} but Map value has version {}",
                         key, keyBuf.remaining(), DataConsistencyTest.getVersionFromValue(mapValue));
        }
        else {
            logger.error("HaloDB value for key {} has version {} of length {} but map value version is {}",
                         key, keyBuf.remaining(), DataConsistencyTest.getVersionFromValue(dbValue), DataConsistencyTest
                             .getVersionFromValue(mapValue));
        }

        return -1;
    }

    boolean containsKey(byte[] key) throws HaloDBException {
        return haloDB.get(key) != null;
    }
}
