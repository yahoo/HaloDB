/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import com.oath.halodb.histo.EstimatedHistogram;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * @author Arjun Mannaly
 */
abstract class Segment<V> {

    final HashTableValueSerializer<V> valueSerializer;
    final int fixedValueLength;
    final int fixedKeyLength;

    private final Hasher hasher;

    private volatile long lock;
    private static final AtomicLongFieldUpdater<Segment> lockFieldUpdater =
        AtomicLongFieldUpdater.newUpdater(Segment.class, "lock");

    Segment(HashTableValueSerializer<V> valueSerializer, int fixedValueLength, Hasher hasher) {
        this(valueSerializer, fixedValueLength, -1, hasher);
    }

    Segment(HashTableValueSerializer<V> valueSerializer, int fixedValueLength, int fixedKeyLength, Hasher hasher) {
        this.valueSerializer = valueSerializer;
        this.fixedValueLength = fixedValueLength;
        this.fixedKeyLength = fixedKeyLength;
        this.hasher = hasher;
    }



    boolean lock() {
        long t = Thread.currentThread().getId();

        if (t == lockFieldUpdater.get(this)) {
            return false;
        }
        while (true) {
            if (lockFieldUpdater.compareAndSet(this, 0L, t)) {
                return true;
            }

            // yield control to other thread.
            // Note: we cannot use LockSupport.parkNanos() as that does not
            // provide nanosecond resolution on Windows.
            Thread.yield();
        }
    }

    void unlock(boolean wasFirst) {
        if (!wasFirst) {
            return;
        }

        long t = Thread.currentThread().getId();
        boolean r = lockFieldUpdater.compareAndSet(this, t, 0L);
        assert r;
    }

    KeyBuffer keySource(byte[] key) {
        KeyBuffer keyBuffer = new KeyBuffer(key);
        return keyBuffer.finish(hasher);
    }

    abstract V getEntry(KeyBuffer key);

    abstract boolean containsEntry(KeyBuffer key);

    abstract boolean putEntry(byte[] key, V value, long hash, boolean ifAbsent, V oldValue);

    abstract boolean removeEntry(KeyBuffer key);

    abstract long size();

    abstract void release();

    abstract void clear();

    abstract long hitCount();

    abstract long missCount();

    abstract long putAddCount();

    abstract long putReplaceCount();

    abstract long removeCount();

    abstract void resetStatistics();

    abstract long rehashes();

    abstract float loadFactor();

    abstract int hashTableSize();

    abstract void updateBucketHistogram(EstimatedHistogram hist);


    //Used only in memory pool.

    long numberOfChunks() {
        return -1;
    }

    long numberOfSlots() {
        return -1;
    }

    long freeListSize() {
        return -1;
    }
}
