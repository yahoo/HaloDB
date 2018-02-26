/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import com.oath.halodb.cache.OHCacheStats;

public interface KeyCache {

    boolean put(byte[] key, RecordMetaDataForCache metaData);

    RecordMetaDataForCache get(byte[] key);

    boolean remove(byte[] key);

    boolean replace(byte[] key, RecordMetaDataForCache oldValue, RecordMetaDataForCache newValue);

    boolean containsKey(byte[] key);

    void close();

    long size();

    OHCacheStats stats();

    void resetStats();

    int getNoOfSegments();

    int getMaxSizeOfEachSegment();
}
