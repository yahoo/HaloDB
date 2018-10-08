/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * On-heap test-only counterpart of {@link SegmentNonMemoryPool} for {@link CheckOffHeapHashTable}.
 */
final class CheckSegment {
    private final Map<KeyBuffer, byte[]> map;
    private final LinkedList<KeyBuffer> lru = new LinkedList<>();

    long hitCount;
    long missCount;
    long putAddCount;
    long putReplaceCount;
    long removeCount;
    long evictedEntries;

    public CheckSegment(int initialCapacity, float loadFactor) {
        this.map = new HashMap<>(initialCapacity, loadFactor);
    }

    synchronized void clear()
    {
        map.clear();
        lru.clear();
    }

    synchronized byte[] get(KeyBuffer keyBuffer)
    {
        byte[] r = map.get(keyBuffer);
        if (r == null)
        {
            missCount++;
            return null;
        }

        lru.remove(keyBuffer);
        lru.addFirst(keyBuffer);
        hitCount++;

        return r;
    }

    synchronized boolean put(KeyBuffer keyBuffer, byte[] data, boolean ifAbsent, byte[] old)
    {
        byte[] existing = map.get(keyBuffer);

        if (ifAbsent && existing != null)
            return false;

        if (old != null && !Arrays.equals(old, existing))  {
            return false;
        }

        map.put(keyBuffer, data);
        lru.remove(keyBuffer);
        lru.addFirst(keyBuffer);

        if (existing != null)
        {
            putReplaceCount++;
        }
        else
            putAddCount++;

        return true;
    }

    synchronized boolean remove(KeyBuffer keyBuffer)
    {
        byte[] old = map.remove(keyBuffer);
        if (old != null)
        {
            boolean r = lru.remove(keyBuffer);
            removeCount++;
            return r;
        }
        return false;
    }

    synchronized long size()
    {
        return map.size();
    }

    static long sizeOf(KeyBuffer key, byte[] value)
    {
        // calculate the same value as the original impl would do
        return NonMemoryPoolHashEntries.ENTRY_OFF_DATA + key.size() + value.length;
    }

    void resetStatistics()
    {
        evictedEntries = 0L;
        hitCount = 0L;
        missCount = 0L;
        putAddCount = 0L;
        putReplaceCount = 0L;
        removeCount = 0L;
    }

}
