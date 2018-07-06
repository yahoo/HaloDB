/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import com.oath.halodb.cache.OHCache;
import com.oath.halodb.cache.linked.OHCacheBuilder;
import com.google.common.primitives.Ints;
import com.oath.halodb.cache.OHCacheStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Hash table stored in native memory, outside Java heap.
 *
 * @author Arjun Mannaly
 */
class OffHeapCache implements KeyCache {
    private static final Logger logger = LoggerFactory.getLogger(OffHeapCache.class);

    private final OHCache<RecordMetaDataForCache> ohCache;

    private final int noOfSegments;
    private final int maxSizeOfEachSegment;

    OffHeapCache(int numberOfKeys, boolean useMemoryPool, int fixedKeySize, int memoryPoolChunkSize) {
        noOfSegments = Ints.checkedCast(Utils.roundUpToPowerOf2(Runtime.getRuntime().availableProcessors() * 2));
        maxSizeOfEachSegment = Ints.checkedCast(Utils.roundUpToPowerOf2(numberOfKeys / noOfSegments));

        long start = System.currentTimeMillis();
        OHCacheBuilder<RecordMetaDataForCache> builder =
            OHCacheBuilder.<RecordMetaDataForCache>newBuilder()
                .valueSerializer(new RecordMetaDataSerializer())
                .capacity(Long.MAX_VALUE)
                .segmentCount(noOfSegments)
                .hashTableSize(maxSizeOfEachSegment)
                .fixedValueSize(RecordMetaDataForCache.SERIALIZED_SIZE)
                .loadFactor(1)
                .throwOOME(true);

        if (useMemoryPool) {
            builder.useMemoryPool(true).fixedKeySize(fixedKeySize).memoryPoolChunkSize(memoryPoolChunkSize);
        }

        this.ohCache = builder.build();

        logger.info("Initialized the cache in {}", (System.currentTimeMillis() - start));
    }

    @Override
    public boolean put(byte[] key, RecordMetaDataForCache metaData) {
        ohCache.put(key, metaData);
        return true;
    }

    @Override
    public boolean remove(byte[] key) {
        return ohCache.remove(key);
    }

    @Override
    public boolean replace(byte[] key, RecordMetaDataForCache oldValue, RecordMetaDataForCache newValue) {
        return ohCache.addOrReplace(key, oldValue, newValue);
    }

    @Override
    public RecordMetaDataForCache get(byte[] key) {
        return ohCache.get(key);
    }

    @Override
    public boolean containsKey(byte[] key) {
        return ohCache.containsKey(key);
    }

    @Override
    public void close() {
        try {
            ohCache.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public long size() {
        return ohCache.size();
    }

    @Override
    public OHCacheStats stats() {
        return ohCache.stats();
    }

    public void resetStats() {
        ohCache.resetStatistics();
    }

    public int getNoOfSegments() {
        return noOfSegments;
    }

    public int getMaxSizeOfEachSegment() {
        return maxSizeOfEachSegment;
    }
}
