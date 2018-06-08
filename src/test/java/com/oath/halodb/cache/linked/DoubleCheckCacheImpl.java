/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb.cache.linked;

import com.oath.halodb.cache.CloseableIterator;
import com.oath.halodb.cache.OHCache;
import com.oath.halodb.cache.OHCacheBuilder;
import com.oath.halodb.cache.OHCacheStats;
import com.oath.halodb.cache.histo.EstimatedHistogram;

import org.testng.Assert;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Test code that contains an instance of the production and check {@link OHCache}
 * implementations {@link OHCacheLinkedImpl} and
 * {@link CheckOHCacheImpl}.
 */
public class DoubleCheckCacheImpl<K, V> implements OHCache<K, V>
{
    public final OHCache<K, V> prod;
    public final OHCache<K, V> check;

    public DoubleCheckCacheImpl(OHCacheBuilder<K, V> builder)
    {
        this.prod = builder.build();
        this.check = new CheckOHCacheImpl<>(builder);
    }

    public boolean put(K key, V value)
    {
        boolean rProd = prod.put(key, value);
        boolean rCheck = check.put(key, value);
        Assert.assertEquals(rProd, rCheck, "for key='" + key + '\'');
        return rProd;
    }

    public boolean addOrReplace(K key, V old, V value)
    {
        boolean rProd = prod.addOrReplace(key, old, value);
        boolean rCheck = check.addOrReplace(key, old, value);
        Assert.assertEquals(rProd, rCheck, "for key='" + key + '\'');
        return rProd;
    }

    public boolean putIfAbsent(K k, V v)
    {
        boolean rProd = prod.putIfAbsent(k, v);
        boolean rCheck = check.putIfAbsent(k, v);
        Assert.assertEquals(rProd, rCheck, "for key='" + k + '\'');
        return rProd;
    }

    public boolean putIfAbsent(K key, V value, long expireAt)
    {
        throw new UnsupportedOperationException();
    }

    public boolean put(K key, V value, long expireAt)
    {
        throw new UnsupportedOperationException();
    }

    public void putAll(Map<? extends K, ? extends V> m)
    {
        prod.putAll(m);
        check.putAll(m);
    }

    public boolean remove(K key)
    {
        boolean rProd = prod.remove(key);
        boolean rCheck = check.remove(key);
        Assert.assertEquals(rCheck, rProd, "for key='" + key + '\'');
        return rProd;
    }

    public void removeAll(Iterable<K> keys)
    {
        prod.removeAll(keys);
        check.removeAll(keys);
    }

    public void clear()
    {
        prod.clear();
        check.clear();
    }

    public V get(K key)
    {
        V rProd = prod.get(key);
        V rCheck = check.get(key);
        Assert.assertEquals(rProd, rCheck, "for key='" + key + '\'');
        return rProd;
    }

    public boolean containsKey(K key)
    {
        boolean rProd = prod.containsKey(key);
        boolean rCheck = check.containsKey(key);
        Assert.assertEquals(rProd, rCheck, "for key='" + key + '\'');
        return rProd;
    }

    public CloseableIterator<K> keyIterator()
    {
        return new CheckIterator<>(
                                   prod.keyIterator(),
                                   check.keyIterator(),
                                   true
        );
    }

    public CloseableIterator<ByteBuffer> keyBufferIterator()
    {
        return new CheckIterator<>(
                                   prod.keyBufferIterator(),
                                   check.keyBufferIterator(),
                                   false
        );
    }

    public boolean deserializeEntry(ReadableByteChannel channel) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public boolean serializeEntry(K key, WritableByteChannel channel) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public int deserializeEntries(ReadableByteChannel channel) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public int serializeHotNEntries(int n, WritableByteChannel channel) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public int serializeHotNKeys(int n, WritableByteChannel channel) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public CloseableIterator<K> deserializeKeys(ReadableByteChannel channel) throws IOException
    {
        throw new UnsupportedOperationException();
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

    public long[] perSegmentSizes()
    {
        long[] rProd = prod.perSegmentSizes();
        long[] rCheck = check.perSegmentSizes();
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

    public long capacity()
    {
        long rProd = prod.capacity();
        long rCheck = check.capacity();
        Assert.assertEquals(rProd, rCheck);
        return rProd;
    }

    public long memUsed()
    {
        long rProd = prod.memUsed();
        long rCheck = check.memUsed();
        Assert.assertEquals(rProd, rCheck);
        return rProd;
    }

    public long freeCapacity()
    {
        long rProd = prod.freeCapacity();
        long rCheck = check.freeCapacity();
        Assert.assertEquals(rProd, rCheck, "capacity: " + capacity());
        return rProd;
    }

    public float loadFactor()
    {
        float rProd = prod.loadFactor();
        float rCheck = check.loadFactor();
        Assert.assertEquals(rProd, rCheck);
        return rProd;
    }

    public OHCacheStats stats()
    {
        OHCacheStats rProd = prod.stats();
        OHCacheStats rCheck = check.stats();
        Assert.assertEquals(rProd, rCheck);
        return rProd;
    }

    public void setCapacity(long capacity)
    {
        prod.setCapacity(capacity);
        check.setCapacity(capacity);
    }

    public void close() throws IOException
    {
        prod.close();
        check.close();
    }

    private class CheckIterator<T> implements CloseableIterator<T>
    {
        private final CloseableIterator<T> prodIter;
        private final CloseableIterator<T> checkIter;

        private final boolean canCompare;

        private final Set<T> prodReturned = new HashSet<>();
        private final Set<T> checkReturned = new HashSet<>();

        CheckIterator(CloseableIterator<T> prodIter, CloseableIterator<T> checkIter, boolean canCompare)
        {
            this.prodIter = prodIter;
            this.checkIter = checkIter;
            this.canCompare = canCompare;
        }

        public void close() throws IOException
        {
            prodIter.close();
            checkIter.close();

            Assert.assertEquals(prodReturned.size(), checkReturned.size());
            if (canCompare)
            {
                for (T t : prodReturned)
                    Assert.assertTrue(check.containsKey((K) t), "check does not contain key " + t);
                for (T t : checkReturned)
                    Assert.assertTrue(prod.containsKey((K) t), "prod does not contain key " + t);
            }
        }

        public boolean hasNext()
        {
            boolean rProd = prodIter.hasNext();
            boolean rCheck = checkIter.hasNext();
            Assert.assertEquals(rProd, rCheck);
            return rProd;
        }

        public T next()
        {
            T rProd = prodIter.next();
            T rCheck = checkIter.next();
            prodReturned.add(rProd);
            checkReturned.add(rCheck);
            return rProd;
        }

        public void remove()
        {
            prodIter.remove();
            checkIter.remove();
        }
    }
}
