/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import com.google.common.primitives.Ints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Hash table stored in native memory, outside Java heap.
 *
 * @author Arjun Mannaly
 */
class InMemoryIndex {
    private static final Logger logger = LoggerFactory.getLogger(InMemoryIndex.class);

    private final OffHeapHashTable<InMemoryIndexMetaData> offHeapHashTable;

    private final int noOfSegments;
    private final int maxSizeOfEachSegment;

    InMemoryIndex(int numberOfKeys, boolean useMemoryPool, int fixedKeySize, int memoryPoolChunkSize) {
        noOfSegments = Ints.checkedCast(Utils.roundUpToPowerOf2(Runtime.getRuntime().availableProcessors() * 2));
        maxSizeOfEachSegment = Ints.checkedCast(Utils.roundUpToPowerOf2(numberOfKeys / noOfSegments));
        long start = System.currentTimeMillis();
        OffHeapHashTableBuilder<InMemoryIndexMetaData> builder =
            OffHeapHashTableBuilder.<InMemoryIndexMetaData>newBuilder()
                .valueSerializer(new InMemoryIndexMetaDataSerializer())
                .capacity(Long.MAX_VALUE)
                .segmentCount(noOfSegments)
                .hashTableSize(maxSizeOfEachSegment)
                .fixedValueSize(InMemoryIndexMetaData.SERIALIZED_SIZE)
                .loadFactor(1);

        if (useMemoryPool) {
            builder.useMemoryPool(true).fixedKeySize(fixedKeySize).memoryPoolChunkSize(memoryPoolChunkSize);
        }

        this.offHeapHashTable = builder.build();

        logger.debug("Allocated memory for the index in {}", (System.currentTimeMillis() - start));
    }

    boolean put(byte[] key, InMemoryIndexMetaData metaData) {
        return offHeapHashTable.put(key, metaData);
    }

    boolean remove(byte[] key) {
        return offHeapHashTable.remove(key);
    }

    boolean replace(byte[] key, InMemoryIndexMetaData oldValue, InMemoryIndexMetaData newValue) {
        return offHeapHashTable.addOrReplace(key, oldValue, newValue);
    }

    InMemoryIndexMetaData get(byte[] key) {
        return offHeapHashTable.get(key);
    }

    boolean containsKey(byte[] key) {
        return offHeapHashTable.containsKey(key);
    }

    void close() {
        try {
            offHeapHashTable.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    long size() {
        return offHeapHashTable.size();
    }

    public OffHeapHashTableStats stats() {
        return offHeapHashTable.stats();
    }

    void resetStats() {
        offHeapHashTable.resetStatistics();
    }

    int getNoOfSegments() {
        return noOfSegments;
    }

    int getMaxSizeOfEachSegment() {
        return maxSizeOfEachSegment;
    }
}
