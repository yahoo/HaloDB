/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import com.google.common.primitives.Longs;
import com.oath.halodb.histo.EstimatedHistogram;

import org.testng.Assert;

import java.io.IOException;

/**
 * Test code that contains an instance of the production and check {@link OffHeapHashTable}
 * implementations {@link OffHeapHashTableImpl} and
 * {@link CheckOffHeapHashTable}.
 */
public class DoubleCheckOffHeapHashTableImpl<V> implements OffHeapHashTable<V>
{
    public final OffHeapHashTable<V> prod;
    public final OffHeapHashTable<V> check;

    public DoubleCheckOffHeapHashTableImpl(OffHeapHashTableBuilder<V> builder)
    {
        this.prod = builder.build();
        this.check = new CheckOffHeapHashTable<>(builder);
    }

    public boolean put(byte[] key, V value)
    {
        boolean rProd = prod.put(key, value);
        boolean rCheck = check.put(key, value);
        Assert.assertEquals(rProd, rCheck, "for key='" + key + '\'');
        return rProd;
    }

    public boolean addOrReplace(byte[] key, V old, V value)
    {
        boolean rProd = prod.addOrReplace(key, old, value);
        boolean rCheck = check.addOrReplace(key, old, value);
        Assert.assertEquals(rProd, rCheck, "for key='" + key + '\'');
        return rProd;
    }

    public boolean putIfAbsent(byte[] k, V v)
    {
        boolean rProd = prod.putIfAbsent(k, v);
        boolean rCheck = check.putIfAbsent(k, v);
        Assert.assertEquals(rProd, rCheck, "for key='" + k + '\'');
        return rProd;
    }

    public boolean putIfAbsent(byte[] key, V value, long expireAt)
    {
        throw new UnsupportedOperationException();
    }

    public boolean put(byte[] key, V value, long expireAt)
    {
        throw new UnsupportedOperationException();
    }

    public boolean remove(byte[] key)
    {
        boolean rProd = prod.remove(key);
        boolean rCheck = check.remove(key);
        Assert.assertEquals(rCheck, rProd, "for key='" + key + '\'');
        return rProd;
    }

    public void clear()
    {
        prod.clear();
        check.clear();
    }

    public V get(byte[] key)
    {
        V rProd = prod.get(key);
        V rCheck = check.get(key);
        Assert.assertEquals(rProd, rCheck, "for key='" + Longs.fromByteArray(key) + '\'');
        return rProd;
    }

    public boolean containsKey(byte[] key)
    {
        boolean rProd = prod.containsKey(key);
        boolean rCheck = check.containsKey(key);
        Assert.assertEquals(rProd, rCheck, "for key='" + key + '\'');
        return rProd;
    }

    public void resetStatistics()
    {
        prod.resetStatistics();
        check.resetStatistics();
    }

    public long size()
    {
        long rProd = prod.size();
        long rCheck = check.size();
        Assert.assertEquals(rProd, rCheck);
        return rProd;
    }

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

    public EstimatedHistogram getBucketHistogram()
    {
        return prod.getBucketHistogram();
    }

    public int segments()
    {
        int rProd = prod.segments();
        int rCheck = check.segments();
        Assert.assertEquals(rProd, rCheck);
        return rProd;
    }

    public float loadFactor()
    {
        float rProd = prod.loadFactor();
        float rCheck = check.loadFactor();
        Assert.assertEquals(rProd, rCheck);
        return rProd;
    }

    public OffHeapHashTableStats stats()
    {
        OffHeapHashTableStats rProd = prod.stats();
        OffHeapHashTableStats rCheck = check.stats();
        Assert.assertEquals(rProd, rCheck);
        return rProd;
    }

    public void close() throws IOException
    {
        prod.close();
        check.close();
    }
}
