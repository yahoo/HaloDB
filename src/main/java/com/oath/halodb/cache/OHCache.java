/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb.cache;

import com.oath.halodb.cache.histo.EstimatedHistogram;
import com.oath.halodb.cache.linked.SegmentStats;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Map;

public interface OHCache<V> extends Closeable {
    /**
     * @param key      key of the entry to be added. Must not be {@code null}.
     * @param value    value of the entry to be added. Must not be {@code null}.
     * @return {@code true}, if the entry has been added, {@code false} otherwise
     */
    boolean put(byte[] key, V value);

    /**
     * Adds key/value if either the key is not present and {@code old} is null or the existing value matches parameter {@code old}.
     *
     * @param key      key of the entry to be added or replaced. Must not be {@code null}.
     * @param old      if the entry exists, it's serialized value is compared to the serialized value of {@code old}
     *                 and only replaced, if it matches.
     * @param value    value of the entry to be added. Must not be {@code null}.
     * @return {@code true} on success or {@code false} if the existing value does not matcuh {@code old}
     */
    boolean addOrReplace(byte[] key, V old, V value);

    /**
     * @param key      key of the entry to be added. Must not be {@code null}.
     * @param value    value of the entry to be added. Must not be {@code null}.
     * @return {@code true} on success or {@code false} if the key is already present.
     */
    boolean putIfAbsent(byte[] key, V value);

    /**
     * Remove a single entry for the given key.
     *
     * @param key key of the entry to be removed. Must not be {@code null}.
     * @return {@code true}, if the entry has been removed, {@code false} otherwise
     */
    boolean remove(byte[] key);

    /**
     * Removes all entries from the cache.
     */
    void clear();

    /**
     * Get the value for a given key.
     *
     * @param key      key of the entry to be retrieved. Must not be {@code null}.
     * @return either the non-{@code null} value or {@code null} if no entry for the requested key exists
     */
    V get(byte[] key);

    /**
     * Checks whether an entry for a given key exists.
     * Usually, this is more efficient than testing for {@code null} via {@link #get(Object)}.
     *
     * @param key      key of the entry to be retrieved. Must not be {@code null}.
     * @return either {@code true} if an entry for the given key exists or {@code false} if no entry for the requested key exists
     */
    boolean containsKey(byte[] key);

    // statistics / information

    void resetStatistics();

    long size();

    int[] hashTableSizes();

    SegmentStats[] perSegmentStats();

    EstimatedHistogram getBucketHistogram();

    int segments();

    long capacity();

    long memUsed();

    long freeCapacity();

    float loadFactor();

    OHCacheStats stats();

    /**
     * Modify the cache's capacity.
     * Lowering the capacity will not immediately remove any entry nor will it immediately free allocated (off heap) memory.
     * <p>
     * Future operations will even allocate in flight, temporary memory - i.e. setting capacity to 0 does not
     * disable the cache, it will continue to work but cannot add more data.
     * </p>
     */
    void setCapacity(long capacity);
}
