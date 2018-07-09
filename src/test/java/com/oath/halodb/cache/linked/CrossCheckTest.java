/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb.cache.linked;

import com.google.common.primitives.Longs;
import com.oath.halodb.cache.HashAlgorithm;
import com.oath.halodb.cache.OHCache;
import com.oath.halodb.cache.OHCacheStats;
import com.oath.halodb.cache.histo.EstimatedHistogram;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

// This unit test uses the production cache implementation and an independent OHCache implementation used to
// cross-check the production implementation.
public class CrossCheckTest
{

    private static final int fixedValueSize = 20;
    private static final int fixedKeySize = 16;

    @AfterMethod(alwaysRun = true)
    public void deinit()
    {
        Uns.clearUnsDebugForTest();
    }

    static DoubleCheckCacheImpl<byte[]> cache(HashAlgorithm hashAlgorithm, boolean useMemoryPool)
    {
        return cache(hashAlgorithm, useMemoryPool, 256);
    }

    static DoubleCheckCacheImpl<byte[]> cache(HashAlgorithm hashAlgorithm, boolean useMemoryPool, long capacity)
    {
        return cache(hashAlgorithm, useMemoryPool, capacity, -1);
    }

    static DoubleCheckCacheImpl<byte[]> cache(HashAlgorithm hashAlgorithm, boolean useMemoryPool, long capacity, int hashTableSize)
    {
        return cache(hashAlgorithm, useMemoryPool, capacity, hashTableSize, -1, -1);
    }

    static DoubleCheckCacheImpl<byte[]> cache(HashAlgorithm hashAlgorithm, boolean useMemoryPool, long capacity, int hashTableSize, int segments, long maxEntrySize)
    {
        OHCacheBuilder<byte[]> builder = OHCacheBuilder.<byte[]>newBuilder()
                                                                .valueSerializer(TestUtils.byteArraySerializer)
                                                                .hashMode(hashAlgorithm)
                                                                .fixedValueSize(fixedValueSize)
                                                                .capacity(Long.MAX_VALUE);
        if (useMemoryPool)
            builder.useMemoryPool(true).fixedKeySize(fixedKeySize);

        if (hashTableSize > 0)
            builder.hashTableSize(hashTableSize);
        if (segments > 0)
            builder.segmentCount(segments);
        else
            // use 16 segments by default to prevent differing test behaviour on varying test hardware
            builder.segmentCount(16);
        if (maxEntrySize > 0)
            builder.maxEntrySize(maxEntrySize);

        return new DoubleCheckCacheImpl<>(builder);
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
        try (OHCache<byte[]> cache = cache(hashAlgorithm, useMemoryPool))
        {
            byte[] key = TestUtils.randomBytes(12);
            byte[] value = TestUtils.randomBytes(fixedValueSize);
            cache.put(key, value);

            byte[] actual = cache.get(key);
            Assert.assertEquals(actual, value);

            cache.remove(key);

            Map<byte[], byte[]> keyValues = new HashMap<>();

            for (int i = 0; i < 100; i++) {
                byte[] k = TestUtils.randomBytes(8);
                byte[] v = TestUtils.randomBytes(fixedValueSize);
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
        try (OHCache<byte[]> cache = cache(hashAlgorithm, useMemoryPool, 64, -1))
        {
            List<TestUtils.KeyValuePair> entries = TestUtils.fillMany(cache, fixedValueSize);

            OHCacheStats stats = cache.stats();
            Assert.assertEquals(stats.getPutAddCount(), TestUtils.manyCount);
            Assert.assertEquals(stats.getSize(), TestUtils.manyCount);

            entries.forEach(kv -> Assert.assertEquals(cache.get(kv.key), kv.value));

            stats = cache.stats();
            Assert.assertEquals(stats.getHitCount(), TestUtils.manyCount);
            Assert.assertEquals(stats.getSize(), TestUtils.manyCount);

            for (int i = 0; i < TestUtils.manyCount; i++)
            {
                TestUtils.KeyValuePair kv = entries.get(i);
                Assert.assertEquals(cache.get(kv.key), kv.value, "for i="+i);
                assertTrue(cache.containsKey(kv.key), "for i="+i);
                byte[] updated = TestUtils.randomBytes(fixedValueSize);
                cache.put(kv.key, updated);
                entries.set(i, new TestUtils.KeyValuePair(kv.key, updated));
                Assert.assertEquals(cache.get(kv.key), updated, "for i="+i);
                Assert.assertEquals(cache.size(), TestUtils.manyCount, "for i="+i);
                assertTrue(cache.containsKey(kv.key), "for i="+i);
            }

            stats = cache.stats();
            Assert.assertEquals(stats.getPutReplaceCount(), TestUtils.manyCount);
            Assert.assertEquals(stats.getSize(), TestUtils.manyCount);

            entries.forEach(kv -> Assert.assertEquals(cache.get(kv.key), kv.value));

            stats = cache.stats();
            Assert.assertEquals(stats.getHitCount(), TestUtils.manyCount * 6);
            Assert.assertEquals(stats.getSize(), TestUtils.manyCount);

            for (int i = 0; i < TestUtils.manyCount; i++)
            {
                TestUtils.KeyValuePair kv = entries.get(i);
                Assert.assertEquals(cache.get(kv.key), kv.value, "for i=" + i);
                assertTrue(cache.containsKey(kv.key), "for i=" + i);
                cache.remove(kv.key);
                Assert.assertNull(cache.get(kv.key), "for i=" + i);
                Assert.assertFalse(cache.containsKey(kv.key), "for i=" + i);
                Assert.assertEquals(cache.stats().getRemoveCount(), i + 1);
                Assert.assertEquals(cache.size(), TestUtils.manyCount - i - 1, "for i=" + i);
            }

            stats = cache.stats();
            Assert.assertEquals(stats.getRemoveCount(), TestUtils.manyCount);
            Assert.assertEquals(stats.getSize(), 0);
        }
    }


    @Test(dataProvider = "hashAlgorithms", dependsOnMethods = "testBasics")
    public void testRehash(HashAlgorithm hashAlgorithm, boolean useMemoryPool) throws IOException, InterruptedException
    {
        int count = 10_000;
        OHCacheBuilder<byte[]> builder = OHCacheBuilder.<byte[]>newBuilder()
            .valueSerializer(TestUtils.byteArraySerializer)
            .hashMode(hashAlgorithm)
            .fixedValueSize(fixedValueSize)
            .capacity(Long.MAX_VALUE)
            .hashTableSize(count/4)
            .segmentCount(1)
            .loadFactor(1);

        if (useMemoryPool)
            builder.useMemoryPool(true).fixedKeySize(fixedKeySize);


        try (OHCache<byte[]> cache = new DoubleCheckCacheImpl<>(builder))
        {
            List<TestUtils.KeyValuePair> entries = TestUtils.fill(cache, fixedValueSize, count);

            OHCacheStats stats = cache.stats();
            Assert.assertEquals(stats.getPutAddCount(), count);
            Assert.assertEquals(stats.getSize(), count);
            Assert.assertEquals(stats.getRehashCount(), 2); // default load factor of 0.75, therefore 3 rehashes.

            entries.forEach(kv -> Assert.assertEquals(cache.get(kv.key), kv.value));

            stats = cache.stats();
            Assert.assertEquals(stats.getHitCount(), count);
            Assert.assertEquals(stats.getSize(), count);

            for (int i = 0; i < count; i++)
            {
                TestUtils.KeyValuePair kv = entries.get(i);
                Assert.assertEquals(cache.get(kv.key), kv.value, "for i="+i);
                assertTrue(cache.containsKey(kv.key), "for i="+i);
                byte[] updated = TestUtils.randomBytes(fixedValueSize);
                cache.put(kv.key, updated);
                entries.set(i, new TestUtils.KeyValuePair(kv.key, updated));
                Assert.assertEquals(cache.get(kv.key), updated, "for i="+i);
                Assert.assertEquals(cache.size(), count, "for i=" + i);
                assertTrue(cache.containsKey(kv.key), "for i="+i);
            }

            stats = cache.stats();
            Assert.assertEquals(stats.getPutReplaceCount(), count);
            Assert.assertEquals(stats.getSize(), count);
            Assert.assertEquals(stats.getRehashCount(), 2);

            entries.forEach(kv -> Assert.assertEquals(cache.get(kv.key), kv.value));

            stats = cache.stats();
            Assert.assertEquals(stats.getHitCount(), count * 6);
            Assert.assertEquals(stats.getSize(), count);

            for (int i = 0; i < count; i++)
            {
                TestUtils.KeyValuePair kv = entries.get(i);
                Assert.assertEquals(cache.get(kv.key), kv.value, "for i=" + i);
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



//
//    private String longString()
//    {
//        char[] chars = new char[900];
//        for (int i = 0; i < chars.length; i++)
//            chars[i] = (char) ('A' + i % 26);
//        return new String(chars);
//    }
//
//
    @Test(dataProvider = "hashAlgorithms", dependsOnMethods = "testBasics",
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = ".*greater than fixed value size.*")
    public void testPutTooLargeValue(HashAlgorithm hashAlgorithm, boolean useMemoryPool) throws IOException, InterruptedException {
        byte[] key = TestUtils.randomBytes(8);
        byte[] largeValue = TestUtils.randomBytes(fixedValueSize + 1);

        try (OHCache<byte[]> cache = cache(hashAlgorithm, useMemoryPool,1, -1)) {
            cache.put(key, largeValue);
        }
    }

    @Test(dataProvider = "hashAlgorithms", dependsOnMethods = "testBasics",
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = ".*exceeds max permitted size of 127")
    public void testPutTooLargeKey(HashAlgorithm hashAlgorithm, boolean useMemoryPool) throws IOException, InterruptedException {
        byte[] key = TestUtils.randomBytes(1024);
        byte[] largeValue = TestUtils.randomBytes(fixedValueSize);

        try (OHCache<byte[]> cache = cache(hashAlgorithm, useMemoryPool, 1, -1)) {
            cache.put(key, largeValue);
        }
    }

    // per-method tests

    @Test(dataProvider = "hashAlgorithms", dependsOnMethods = "testBasics")
    public void testAddOrReplace(HashAlgorithm hashAlgorithm, boolean useMemoryPool) throws Exception
    {
        try (OHCache<byte[]> cache = cache(hashAlgorithm, useMemoryPool))
        {
            byte[] oldValue = null;
            for (int i = 0; i < TestUtils.manyCount; i++)
                assertTrue(cache.addOrReplace(Longs.toByteArray(i), oldValue, TestUtils.randomBytes(fixedValueSize)));

            byte[] key = Longs.toByteArray(42);
            byte[] value = cache.get(key);
            byte[] update1 = TestUtils.randomBytes(fixedValueSize);
            assertTrue(cache.addOrReplace(key, value, update1));
            Assert.assertEquals(cache.get(key), update1);

            byte[] update2 = TestUtils.randomBytes(fixedValueSize);
            assertTrue(cache.addOrReplace(key, update1, update2));
            Assert.assertEquals(cache.get(key), update2);
            Assert.assertFalse(cache.addOrReplace(key, update1, update2));
            Assert.assertEquals(cache.get(key), update2);

            cache.remove(key);
            Assert.assertNull(cache.get(key));

            byte[] update3 = TestUtils.randomBytes(fixedValueSize);

            // update will fail since the key was removed but old value is non-null.
            Assert.assertFalse(cache.addOrReplace(key, update2, update3));
            Assert.assertNull(cache.get(key));
        }
    }

    @Test(dataProvider = "hashAlgorithms")
    public void testPutIfAbsent(HashAlgorithm hashAlgorithm, boolean useMemoryPool) throws Exception
    {
        try (OHCache<byte[]> cache = cache(hashAlgorithm, useMemoryPool))
        {
            for (int i = 0; i < TestUtils.manyCount; i++)
                assertTrue(cache.putIfAbsent(Longs.toByteArray(i), TestUtils.randomBytes(fixedValueSize)));

            byte[] key = Longs.toByteArray(TestUtils.manyCount + 100);
            byte[] value = TestUtils.randomBytes(fixedValueSize);
            assertTrue(cache.putIfAbsent(key, value));
            Assert.assertEquals(cache.get(key), value);
            Assert.assertFalse(cache.putIfAbsent(key, value));
        }
    }

    @Test(dataProvider = "hashAlgorithms")
    public void testRemove(HashAlgorithm hashAlgorithm, boolean useMemoryPool) throws Exception
    {
        try (OHCache<byte[]> cache = cache(hashAlgorithm, useMemoryPool))
        {
            TestUtils.fillMany(cache, fixedValueSize);

            byte[] key = Longs.toByteArray(TestUtils.manyCount + 100);
            byte[] value = TestUtils.randomBytes(fixedValueSize);
            cache.put(key, value);
            Assert.assertEquals(cache.get(key), value);
            cache.remove(key);
            Assert.assertNull(cache.get(key));
            Assert.assertFalse(cache.remove(key));

            Random r = new Random();
            for (int i = 0; i < TestUtils.manyCount; i++)
                cache.remove(Longs.toByteArray(r.nextInt(TestUtils.manyCount)));
        }
    }

    @Test(dataProvider = "hashAlgorithms")
    public void testClear(HashAlgorithm hashAlgorithm, boolean useMemoryPool) throws Exception
    {
        try (OHCache<byte[]> cache = cache(hashAlgorithm, useMemoryPool))
        {
            List<TestUtils.KeyValuePair> data = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                data.add(new TestUtils.KeyValuePair(Longs.toByteArray(i), TestUtils.randomBytes(fixedValueSize)));
            }

            data.forEach(kv -> cache.put(kv.key, kv.value));
            data.forEach(kv -> Assert.assertEquals(cache.get(kv.key), kv.value));

            assertEquals(cache.size(), 100);

            cache.clear();
            assertEquals(cache.size(), 0);
        }
    }

    @Test(dataProvider = "hashAlgorithms")
    public void testGet_Put(HashAlgorithm hashAlgorithm, boolean useMemoryPool) throws Exception
    {
        try (OHCache<byte[]> cache = cache(hashAlgorithm, useMemoryPool))
        {
            byte[] key = Longs.toByteArray(42);
            byte[] value = TestUtils.randomBytes(fixedValueSize);
            cache.put(key, value);
            assertEquals(cache.get(key), value);
            Assert.assertNull(cache.get(Longs.toByteArray(5)));

            byte[] key11 = Longs.toByteArray(11);
            byte[] value11 = TestUtils.randomBytes(fixedValueSize);
            cache.put(key11, value11);
            Assert.assertEquals(cache.get(key), value);
            Assert.assertEquals(cache.get(key11), value11);

            value11 = TestUtils.randomBytes(fixedValueSize);
            cache.put(key11, value11);
            Assert.assertEquals(cache.get(key), value);
            Assert.assertEquals(cache.get(key11), value11);
        }
    }

    @Test(dataProvider = "hashAlgorithms")
    public void testContainsKey(HashAlgorithm hashAlgorithm, boolean useMemoryPool) throws Exception
    {
        try (OHCache<byte[]> cache = cache(hashAlgorithm, useMemoryPool))
        {
            byte[] key = Longs.toByteArray(42);
            byte[] value = TestUtils.randomBytes(fixedValueSize);
            cache.put(key, value);
            assertTrue(cache.containsKey(key));
            Assert.assertFalse(cache.containsKey(Longs.toByteArray(11)));
        }
    }


    @Test(dataProvider = "hashAlgorithms")
    public void testGetBucketHistogram(HashAlgorithm hashAlgorithm, boolean useMemoryPool) throws Exception
    {
        try (DoubleCheckCacheImpl<byte[]> cache = cache(hashAlgorithm, useMemoryPool))
        {
            List<TestUtils.KeyValuePair> data = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                data.add(new TestUtils.KeyValuePair(Longs.toByteArray(i), TestUtils.randomBytes(fixedValueSize)));
            }

            data.forEach(kv -> cache.put(kv.key, kv.value));

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
        try (OHCache<byte[]> cache = cache(hashAlgorithm, useMemoryPool))
        {
            for (int i = 0; i < 100; i++)
                cache.put(Longs.toByteArray(i), TestUtils.randomBytes(fixedValueSize));

            for (int i = 0; i < 30; i++)
                cache.put(Longs.toByteArray(i), TestUtils.randomBytes(fixedValueSize));

            for (int i = 0; i < 50; i++)
                cache.get(Longs.toByteArray(i));

            for (int i = 100; i < 120; i++)
                cache.get(Longs.toByteArray(i));

            for (int i = 0; i < 25; i++)
                cache.remove(Longs.toByteArray(i));

            OHCacheStats stats = cache.stats();
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
