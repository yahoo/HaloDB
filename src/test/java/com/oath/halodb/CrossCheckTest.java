/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.primitives.Longs;
import com.oath.halodb.HashTableTestUtils.KeyEntryPair;
import com.oath.halodb.histo.EstimatedHistogram;

// This unit test uses the production cache implementation and an independent OHCache implementation used to
// cross-check the production implementation.
public class CrossCheckTest
{

    private static final int fixedKeySize = 16;
    private static final ByteArrayEntrySerializer serializer = ByteArrayEntrySerializer.ofSize(14);
    private static final ByteArrayEntrySerializer bigSerializer = ByteArrayEntrySerializer.ofSize(15);

    @AfterMethod(alwaysRun = true)
    public void deinit()
    {
        Uns.clearUnsDebugForTest();
    }

    static DoubleCheckOffHeapHashTableImpl<ByteArrayEntry> cache(HashAlgorithm hashAlgorithm, boolean useMemoryPool)
    {
        return cache(hashAlgorithm, useMemoryPool, 256);
    }

    static DoubleCheckOffHeapHashTableImpl<ByteArrayEntry> cache(HashAlgorithm hashAlgorithm, boolean useMemoryPool, long capacity)
    {
        return cache(hashAlgorithm, useMemoryPool, capacity, -1);
    }

    static DoubleCheckOffHeapHashTableImpl<ByteArrayEntry> cache(HashAlgorithm hashAlgorithm, boolean useMemoryPool, long capacity, int hashTableSize)
    {
        return cache(hashAlgorithm, useMemoryPool, capacity, hashTableSize, -1, -1);
    }

    static DoubleCheckOffHeapHashTableImpl<ByteArrayEntry> cache(HashAlgorithm hashAlgorithm, boolean useMemoryPool, long capacity, int hashTableSize, int segments, long maxEntrySize)
    {
        OffHeapHashTableBuilder<ByteArrayEntry> builder = OffHeapHashTableBuilder.newBuilder(serializer)
                                                                .hashMode(hashAlgorithm);
        if (useMemoryPool)
            builder.useMemoryPool(true).fixedKeySize(fixedKeySize);

        if (hashTableSize > 0)
            builder.hashTableSize(hashTableSize);
        if (segments > 0)
            builder.segmentCount(segments);
        else
            // use 16 segments by default to prevent differing test behaviour on varying test hardware
            builder.segmentCount(16);

        return new DoubleCheckOffHeapHashTableImpl<>(builder);
    }

    @DataProvider(name = "hashAlgorithms")
    public Object[][] cacheEviction()
    {
        return new Object[][]{
            {HashAlgorithm.MURMUR3, false },
            {HashAlgorithm.MURMUR3, true },
            {HashAlgorithm.CRC32, false },
            {HashAlgorithm.CRC32, true },
            {HashAlgorithm.XX, false },
            {HashAlgorithm.XX, true }
        };
    }


    @Test(dataProvider = "hashAlgorithms")
    public void testBasics(HashAlgorithm hashAlgorithm, boolean useMemoryPool) throws IOException, InterruptedException
    {
        try (OffHeapHashTable<ByteArrayEntry> cache = cache(hashAlgorithm, useMemoryPool))
        {
            byte[] key = HashTableTestUtils.randomBytes(12);
            ByteArrayEntry value = serializer.randomEntry(key.length);
            cache.put(key, value);

            ByteArrayEntry actual = cache.get(key);
            Assert.assertEquals(actual, value);

            cache.remove(key);

            Map<byte[], ByteArrayEntry> keyValues = new HashMap<>();

            for (int i = 0; i < 100; i++) {
                byte[] k = HashTableTestUtils.randomBytes(8);
                ByteArrayEntry v = serializer.randomEntry(k.length);
                keyValues.put(k, v);
                cache.put(k, v);
            }

            keyValues.forEach((k, v) -> {
                Assert.assertEquals(cache.get(k), v);
            });

            // implicitly compares stats
            cache.stats();
        }
    }

    @Test(dataProvider = "hashAlgorithms", dependsOnMethods = "testBasics")
    public void testManyValues(HashAlgorithm hashAlgorithm, boolean useMemoryPool) throws IOException, InterruptedException
    {
        try (OffHeapHashTable<ByteArrayEntry> cache = cache(hashAlgorithm, useMemoryPool, 64, -1))
        {
            List<KeyEntryPair> entries = HashTableTestUtils.fillMany(cache, serializer);

            OffHeapHashTableStats stats = cache.stats();
            Assert.assertEquals(stats.getPutAddCount(), HashTableTestUtils.manyCount);
            Assert.assertEquals(stats.getSize(), HashTableTestUtils.manyCount);

            entries.forEach(kv -> Assert.assertEquals(cache.get(kv.key), kv.entry));

            stats = cache.stats();
            Assert.assertEquals(stats.getHitCount(), HashTableTestUtils.manyCount);
            Assert.assertEquals(stats.getSize(), HashTableTestUtils.manyCount);

            for (int i = 0; i < HashTableTestUtils.manyCount; i++)
            {
                KeyEntryPair kv = entries.get(i);
                Assert.assertEquals(cache.get(kv.key), kv.entry, "for i="+i);
                assertTrue(cache.containsKey(kv.key), "for i="+i);
                ByteArrayEntry updated = serializer.randomEntry(kv.key.length);
                cache.put(kv.key, updated);
                entries.set(i, new KeyEntryPair(kv.key, updated));
                Assert.assertEquals(cache.get(kv.key), updated, "for i="+i);
                Assert.assertEquals(cache.size(), HashTableTestUtils.manyCount, "for i=" + i);
                assertTrue(cache.containsKey(kv.key), "for i="+i);
            }

            stats = cache.stats();
            Assert.assertEquals(stats.getPutReplaceCount(), HashTableTestUtils.manyCount);
            Assert.assertEquals(stats.getSize(), HashTableTestUtils.manyCount);

            entries.forEach(kv -> Assert.assertEquals(cache.get(kv.key), kv.entry));

            stats = cache.stats();
            Assert.assertEquals(stats.getHitCount(), HashTableTestUtils.manyCount * 6);
            Assert.assertEquals(stats.getSize(), HashTableTestUtils.manyCount);

            for (int i = 0; i < HashTableTestUtils.manyCount; i++)
            {
                KeyEntryPair kv = entries.get(i);
                Assert.assertEquals(cache.get(kv.key), kv.entry, "for i=" + i);
                assertTrue(cache.containsKey(kv.key), "for i=" + i);
                cache.remove(kv.key);
                Assert.assertNull(cache.get(kv.key), "for i=" + i);
                Assert.assertFalse(cache.containsKey(kv.key), "for i=" + i);
                Assert.assertEquals(cache.stats().getRemoveCount(), i + 1);
                Assert.assertEquals(cache.size(), HashTableTestUtils.manyCount - i - 1, "for i=" + i);
            }

            stats = cache.stats();
            Assert.assertEquals(stats.getRemoveCount(), HashTableTestUtils.manyCount);
            Assert.assertEquals(stats.getSize(), 0);
        }
    }


    @Test(dataProvider = "hashAlgorithms", dependsOnMethods = "testBasics")
    public void testRehash(HashAlgorithm hashAlgorithm, boolean useMemoryPool) throws IOException, InterruptedException
    {
        int count = 10_000;
        OffHeapHashTableBuilder<ByteArrayEntry> builder = OffHeapHashTableBuilder.newBuilder(serializer)
            .hashMode(hashAlgorithm)
            .hashTableSize(count/4)
            .segmentCount(1)
            .loadFactor(1);

        if (useMemoryPool)
            builder.useMemoryPool(true).fixedKeySize(fixedKeySize);


        try (OffHeapHashTable<ByteArrayEntry> cache = new DoubleCheckOffHeapHashTableImpl<>(builder))
        {
            List<KeyEntryPair> entries = HashTableTestUtils.fill(cache, serializer, count);

            OffHeapHashTableStats stats = cache.stats();
            Assert.assertEquals(stats.getPutAddCount(), count);
            Assert.assertEquals(stats.getSize(), count);
            Assert.assertEquals(stats.getRehashCount(), 2); // default load factor of 0.75, therefore 3 rehashes.

            entries.forEach(kv -> Assert.assertEquals(cache.get(kv.key), kv.entry));

            stats = cache.stats();
            Assert.assertEquals(stats.getHitCount(), count);
            Assert.assertEquals(stats.getSize(), count);

            for (int i = 0; i < count; i++)
            {
                KeyEntryPair kv = entries.get(i);
                Assert.assertEquals(cache.get(kv.key), kv.entry, "for i="+i);
                assertTrue(cache.containsKey(kv.key), "for i="+i);
                ByteArrayEntry updated = serializer.randomEntry(kv.key.length);
                cache.put(kv.key, updated);
                entries.set(i, new KeyEntryPair(kv.key, updated));
                Assert.assertEquals(cache.get(kv.key), updated, "for i="+i);
                Assert.assertEquals(cache.size(), count, "for i=" + i);
                assertTrue(cache.containsKey(kv.key), "for i="+i);
            }

            stats = cache.stats();
            Assert.assertEquals(stats.getPutReplaceCount(), count);
            Assert.assertEquals(stats.getSize(), count);
            Assert.assertEquals(stats.getRehashCount(), 2);

            entries.forEach(kv -> Assert.assertEquals(cache.get(kv.key), kv.entry));

            stats = cache.stats();
            Assert.assertEquals(stats.getHitCount(), count * 6);
            Assert.assertEquals(stats.getSize(), count);

            for (int i = 0; i < count; i++)
            {
                KeyEntryPair kv = entries.get(i);
                Assert.assertEquals(cache.get(kv.key), kv.entry, "for i=" + i);
                assertTrue(cache.containsKey(kv.key), "for i=" + i);
                cache.remove(kv.key);
                Assert.assertNull(cache.get(kv.key), "for i=" + i);
                Assert.assertFalse(cache.containsKey(kv.key), "for i=" + i);
                Assert.assertEquals(cache.stats().getRemoveCount(), i + 1);
                Assert.assertEquals(cache.size(), count - i - 1, "for i=" + i);
            }

            stats = cache.stats();
            Assert.assertEquals(stats.getRemoveCount(), count);
            Assert.assertEquals(stats.getSize(), 0);
            Assert.assertEquals(stats.getRehashCount(), 2);
        }
    }

    @Test(dataProvider = "hashAlgorithms", dependsOnMethods = "testBasics",
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = ".*value size incompatible with fixed value size.*")
    public void testPutTooLargeValue(HashAlgorithm hashAlgorithm, boolean useMemoryPool) throws IOException, InterruptedException {
        byte[] key = HashTableTestUtils.randomBytes(8);
        ByteArrayEntry largeEntry = bigSerializer.randomEntry(key.length);

        try (OffHeapHashTable<ByteArrayEntry> cache = cache(hashAlgorithm, useMemoryPool, 1, -1)) {
            cache.put(key, largeEntry);
        }
    }

    @Test(dataProvider = "hashAlgorithms", dependsOnMethods = "testBasics",
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = ".*exceeds max permitted size of 127")
    public void testPutTooLargeKey(HashAlgorithm hashAlgorithm, boolean useMemoryPool) throws IOException, InterruptedException {
        byte[] key = HashTableTestUtils.randomBytes(1024);
        ByteArrayEntry largeEntry = bigSerializer.randomEntry(key.length);

        try (OffHeapHashTable<ByteArrayEntry> cache = cache(hashAlgorithm, useMemoryPool, 1, -1)) {
            cache.put(key, largeEntry);
        }
    }

    // per-method tests

    @Test(dataProvider = "hashAlgorithms", dependsOnMethods = "testBasics")
    public void testAddOrReplace(HashAlgorithm hashAlgorithm, boolean useMemoryPool) throws Exception
    {
        try (OffHeapHashTable<ByteArrayEntry> cache = cache(hashAlgorithm, useMemoryPool))
        {
            ByteArrayEntry oldEntry = null;
            for (int i = 0; i < HashTableTestUtils.manyCount; i++)
                assertTrue(cache.addOrReplace(Longs.toByteArray(i), oldEntry, serializer.randomEntry(8)));

            byte[] key = Longs.toByteArray(42);
            ByteArrayEntry entry = cache.get(key);
            ByteArrayEntry update1 = serializer.randomEntry(key.length);
            assertTrue(cache.addOrReplace(key, entry, update1));
            Assert.assertEquals(cache.get(key), update1);

            ByteArrayEntry update2 = serializer.randomEntry(key.length);
            assertTrue(cache.addOrReplace(key, update1, update2));
            Assert.assertEquals(cache.get(key), update2);
            Assert.assertFalse(cache.addOrReplace(key, update1, update2));
            Assert.assertEquals(cache.get(key), update2);

            cache.remove(key);
            Assert.assertNull(cache.get(key));

            ByteArrayEntry update3 = serializer.randomEntry(key.length);

            // update will fail since the key was removed but old value is non-null.
            Assert.assertFalse(cache.addOrReplace(key, update2, update3));
            Assert.assertNull(cache.get(key));
        }
    }

    @Test(dataProvider = "hashAlgorithms")
    public void testPutIfAbsent(HashAlgorithm hashAlgorithm, boolean useMemoryPool) throws Exception
    {
        try (OffHeapHashTable<ByteArrayEntry> cache = cache(hashAlgorithm, useMemoryPool))
        {
            for (int i = 0; i < HashTableTestUtils.manyCount; i++)
                assertTrue(cache.putIfAbsent(Longs.toByteArray(i), serializer.randomEntry(8)));

            byte[] key = Longs.toByteArray(HashTableTestUtils.manyCount + 100);
            ByteArrayEntry entry = serializer.randomEntry(key.length);
            assertTrue(cache.putIfAbsent(key, entry));
            Assert.assertEquals(cache.get(key), entry);
            Assert.assertFalse(cache.putIfAbsent(key, entry));
        }
    }

    @Test(dataProvider = "hashAlgorithms")
    public void testRemove(HashAlgorithm hashAlgorithm, boolean useMemoryPool) throws Exception
    {
        try (OffHeapHashTable<ByteArrayEntry> cache = cache(hashAlgorithm, useMemoryPool))
        {
            HashTableTestUtils.fillMany(cache, serializer);

            byte[] key = Longs.toByteArray(HashTableTestUtils.manyCount + 100);
            ByteArrayEntry entry = serializer.randomEntry(key.length);
            cache.put(key, entry);
            Assert.assertEquals(cache.get(key), entry);
            cache.remove(key);
            Assert.assertNull(cache.get(key));
            Assert.assertFalse(cache.remove(key));

            Random r = new Random();
            for (int i = 0; i < HashTableTestUtils.manyCount; i++)
                cache.remove(Longs.toByteArray(r.nextInt(HashTableTestUtils.manyCount)));
        }
    }

    @Test(dataProvider = "hashAlgorithms")
    public void testClear(HashAlgorithm hashAlgorithm, boolean useMemoryPool) throws Exception
    {
        try (OffHeapHashTable<ByteArrayEntry> cache = cache(hashAlgorithm, useMemoryPool))
        {
            List<KeyEntryPair> data = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                data.add(new KeyEntryPair(Longs.toByteArray(i), serializer.randomEntry(8)));
            }

            data.forEach(kv -> cache.put(kv.key, kv.entry));
            data.forEach(kv -> Assert.assertEquals(cache.get(kv.key), kv.entry));

            assertEquals(cache.size(), 100);

            cache.clear();
            assertEquals(cache.size(), 0);
        }
    }

    @Test(dataProvider = "hashAlgorithms")
    public void testGet_Put(HashAlgorithm hashAlgorithm, boolean useMemoryPool) throws Exception
    {
        try (OffHeapHashTable<ByteArrayEntry> cache = cache(hashAlgorithm, useMemoryPool))
        {
            byte[] key = Longs.toByteArray(42);
            ByteArrayEntry entry = serializer.randomEntry(key.length);
            cache.put(key, entry);
            assertEquals(cache.get(key), entry);
            Assert.assertNull(cache.get(Longs.toByteArray(5)));

            byte[] key11 = Longs.toByteArray(11);
            ByteArrayEntry entry11 = serializer.randomEntry(key11.length);
            cache.put(key11, entry11);
            Assert.assertEquals(cache.get(key), entry);
            Assert.assertEquals(cache.get(key11), entry11);

            entry11 = serializer.randomEntry(key11.length);
            cache.put(key11, entry11);
            Assert.assertEquals(cache.get(key), entry);
            Assert.assertEquals(cache.get(key11), entry11);
        }
    }

    @Test(dataProvider = "hashAlgorithms")
    public void testContainsKey(HashAlgorithm hashAlgorithm, boolean useMemoryPool) throws Exception
    {
        try (OffHeapHashTable<ByteArrayEntry> cache = cache(hashAlgorithm, useMemoryPool))
        {
            byte[] key = Longs.toByteArray(42);
            ByteArrayEntry entry = serializer.randomEntry(key.length);
            cache.put(key, entry);
            assertTrue(cache.containsKey(key));
            Assert.assertFalse(cache.containsKey(Longs.toByteArray(11)));
        }
    }


    @Test(dataProvider = "hashAlgorithms")
    public void testGetBucketHistogram(HashAlgorithm hashAlgorithm, boolean useMemoryPool) throws Exception
    {
        try (DoubleCheckOffHeapHashTableImpl<ByteArrayEntry> cache = cache(hashAlgorithm, useMemoryPool))
        {
            List<KeyEntryPair> data = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                data.add(new KeyEntryPair(Longs.toByteArray(i), serializer.randomEntry(8)));
            }

            data.forEach(kv -> cache.put(kv.key, kv.entry));

            Assert.assertEquals(cache.stats().getSize(), 100);

            EstimatedHistogram hProd = cache.prod.getBucketHistogram();
            Assert.assertEquals(hProd.count(), sum(cache.prod.hashTableSizes()));
            long[] offsets = hProd.getBucketOffsets();
            Assert.assertEquals(offsets.length, 3);
            Assert.assertEquals(offsets[0], -1);
            Assert.assertEquals(offsets[1], 0);
            Assert.assertEquals(offsets[2], 1);
            // hProd.log(LoggerFactory.getLogger(CrossCheckTest.class));
            // System.out.println(Arrays.toString(offsets));
            Assert.assertEquals(hProd.min(), 0);
            Assert.assertEquals(hProd.max(), 1);
        }
    }

    private static int sum(int[] ints)
    {
        int r = 0;
        for (int i : ints)
            r += i;
        return r;
    }

    @Test(dataProvider = "hashAlgorithms")
    public void testResetStatistics(HashAlgorithm hashAlgorithm, boolean useMemoryPool) throws IOException
    {
        try (OffHeapHashTable<ByteArrayEntry> cache = cache(hashAlgorithm, useMemoryPool))
        {
            for (int i = 0; i < 100; i++)
                cache.put(Longs.toByteArray(i), serializer.randomEntry(8));

            for (int i = 0; i < 30; i++)
                cache.put(Longs.toByteArray(i), serializer.randomEntry(8));

            for (int i = 0; i < 50; i++)
                cache.get(Longs.toByteArray(i));

            for (int i = 100; i < 120; i++)
                cache.get(Longs.toByteArray(i));

            for (int i = 0; i < 25; i++)
                cache.remove(Longs.toByteArray(i));

            OffHeapHashTableStats stats = cache.stats();
            Assert.assertEquals(stats.getPutAddCount(), 100);
            Assert.assertEquals(stats.getPutReplaceCount(), 30);
            Assert.assertEquals(stats.getHitCount(), 50);
            Assert.assertEquals(stats.getMissCount(), 20);
            Assert.assertEquals(stats.getRemoveCount(), 25);

            cache.resetStatistics();

            stats = cache.stats();
            Assert.assertEquals(stats.getPutAddCount(), 0);
            Assert.assertEquals(stats.getPutReplaceCount(), 0);
            Assert.assertEquals(stats.getHitCount(), 0);
            Assert.assertEquals(stats.getMissCount(), 0);
            Assert.assertEquals(stats.getRemoveCount(), 0);
        }
    }
}
