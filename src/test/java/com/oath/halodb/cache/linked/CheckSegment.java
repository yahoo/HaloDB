/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb.cache.linked;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * On-heap test-only counterpart of {@link OffHeapLinkedMap} for {@link CheckOHCacheImpl}.
 */
final class CheckSegment
{
    private final Map<HeapKeyBuffer, byte[]> map;
    private final LinkedList<HeapKeyBuffer> lru = new LinkedList<>();
    private final AtomicLong freeCapacity;

    long hitCount;
    long missCount;
    long putAddCount;
    long putReplaceCount;
    long removeCount;
    long evictedEntries;

    public CheckSegment(int initialCapacity, float loadFactor, AtomicLong freeCapacity)
    {
        this.map = new HashMap<>(initialCapacity, loadFactor);
        this.freeCapacity = freeCapacity;
    }

    synchronized void clear()
    {
        for (Map.Entry<HeapKeyBuffer, byte[]> entry : map.entrySet())
            freeCapacity.addAndGet(sizeOf(entry.getKey(), entry.getValue()));
        map.clear();
        lru.clear();
    }

    synchronized byte[] get(HeapKeyBuffer keyBuffer)
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

    synchronized boolean put(HeapKeyBuffer keyBuffer, byte[] data, boolean ifAbsent, byte[] old)
    {
        long sz = sizeOf(keyBuffer, data);
        while (freeCapacity.get() < sz) {
                remove(keyBuffer);
                return false;
            }

        byte[] existing = map.get(keyBuffer);
        if (ifAbsent || old != null)
        {
            if (ifAbsent && existing != null)
                return false;
            if (old != null && existing != null && !Arrays.equals(old, existing))
                return false;
        }

        map.put(keyBuffer, data);
        lru.remove(keyBuffer);
        lru.addFirst(keyBuffer);

        if (existing != null)
        {
            freeCapacity.addAndGet(sizeOf(keyBuffer, existing));
            putReplaceCount++;
        }
        else
            putAddCount++;

        freeCapacity.addAndGet(-sz);

        return true;
    }

    synchronized boolean remove(HeapKeyBuffer keyBuffer)
    {
        byte[] old = map.remove(keyBuffer);
        if (old != null)
        {
            boolean r = lru.remove(keyBuffer);
            removeCount++;
            freeCapacity.addAndGet(sizeOf(keyBuffer, old));
            return r;
        }
        return false;
    }

    synchronized long size()
    {
        return map.size();
    }

    synchronized Iterator<HeapKeyBuffer> hotN(int n)
    {
        List<HeapKeyBuffer> lst = new ArrayList<>(n);
        for (Iterator<HeapKeyBuffer> iter = lru.iterator(); iter.hasNext() && n-- > 0; )
            lst.add(iter.next());
        return lst.iterator();
    }

    synchronized Iterator<HeapKeyBuffer> keyIterator()
    {
        return new ArrayList<>(lru).iterator();
    }

    //

    static long sizeOf(HeapKeyBuffer key, byte[] value)
    {
        // calculate the same value as the original impl would do
        return HashEntries.ENTRY_OFF_DATA + key.size() + value.length;
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
