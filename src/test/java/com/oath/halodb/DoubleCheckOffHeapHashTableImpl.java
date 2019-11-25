/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import java.io.IOException;

import org.testng.Assert;

import com.google.common.primitives.Longs;
import com.oath.halodb.histo.EstimatedHistogram;

/**
 * Test code that contains an instance of the production and check {@link OffHeapHashTable}
 * implementations {@link OffHeapHashTableImpl} and
 * {@link CheckOffHeapHashTable}.
 */
public class DoubleCheckOffHeapHashTableImpl<E extends HashEntry> implements OffHeapHashTable<E>
{
    public final OffHeapHashTable<E> prod;
    public final OffHeapHashTable<E> check;

    public DoubleCheckOffHeapHashTableImpl(OffHeapHashTableBuilder<E> builder)
    {
        this.prod = builder.build();
        this.check = new CheckOffHeapHashTable<>(builder);
    }

    @Override
    public boolean put(byte[] key, E entry)
    {
        boolean rProd = prod.put(key, entry);
        boolean rCheck = check.put(key, entry);
        Assert.assertEquals(rProd, rCheck, "for key='" + key + '\'');
        return rProd;
    }

    @Override
    public boolean addOrReplace(byte[] key, E old, E entry)
    {
        boolean rProd = prod.addOrReplace(key, old, entry);
        boolean rCheck = check.addOrReplace(key, old, entry);
        Assert.assertEquals(rProd, rCheck, "for key='" + key + '\'');
        return rProd;
    }

    @Override
    public boolean putIfAbsent(byte[] k, E v)
    {
        boolean rProd = prod.putIfAbsent(k, v);
        boolean rCheck = check.putIfAbsent(k, v);
        Assert.assertEquals(rProd, rCheck, "for key='" + k + '\'');
        return rProd;
    }

    public boolean putIfAbsent(byte[] key, E entry, long expireAt)
    {
        throw new UnsupportedOperationException();
    }

    public boolean put(byte[] key, E entry, long expireAt)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(byte[] key)
    {
        boolean rProd = prod.remove(key);
        boolean rCheck = check.remove(key);
        Assert.assertEquals(rCheck, rProd, "for key='" + key + '\'');
        return rProd;
    }

    @Override
    public void clear()
    {
        prod.clear();
        check.clear();
    }

    @Override
    public E get(byte[] key)
    {
        E rProd = prod.get(key);
        E rCheck = check.get(key);
        Assert.assertEquals(rProd, rCheck, "for key='" + Longs.fromByteArray(key) + '\'');
        return rProd;
    }

    @Override
    public boolean containsKey(byte[] key)
    {
        boolean rProd = prod.containsKey(key);
        boolean rCheck = check.containsKey(key);
        Assert.assertEquals(rProd, rCheck, "for key='" + key + '\'');
        return rProd;
    }

    @Override
    public void resetStatistics()
    {
        prod.resetStatistics();
        check.resetStatistics();
    }

    @Override
    public long size()
    {
        long rProd = prod.size();
        long rCheck = check.size();
        Assert.assertEquals(rProd, rCheck);
        return rProd;
    }

    @Override
    public int[] hashTableSizes()
    {
        return prod.hashTableSizes();
    }

    @Override
    public SegmentStats[] perSegmentStats() {
        SegmentStats[] rProd = prod.perSegmentStats();
        SegmentStats[] rCheck = check.perSegmentStats();
        Assert.assertEquals(rProd, rCheck);
        return rProd;
    }

    @Override
    public EstimatedHistogram getBucketHistogram()
    {
        return prod.getBucketHistogram();
    }

    @Override
    public int segments()
    {
        int rProd = prod.segments();
        int rCheck = check.segments();
        Assert.assertEquals(rProd, rCheck);
        return rProd;
    }

    @Override
    public float loadFactor()
    {
        float rProd = prod.loadFactor();
        float rCheck = check.loadFactor();
        Assert.assertEquals(rProd, rCheck);
        return rProd;
    }

    @Override
    public OffHeapHashTableStats stats()
    {
        OffHeapHashTableStats rProd = prod.stats();
        OffHeapHashTableStats rCheck = check.stats();
        Assert.assertEquals(rProd, rCheck);
        return rProd;
    }

    @Override
    public void close() throws IOException
    {
        prod.close();
        check.close();
    }
}
