/*
 *      Copyright (C) 2014 Robert Stupp, Koeln, Germany, robert-stupp.de
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package amannaly.ohc.linked;

import amannaly.ByteArraySerializer;
import com.google.common.primitives.Longs;
import amannaly.ohc.*;
import amannaly.ohc.histo.EstimatedHistogram;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import static org.testng.Assert.*;

// This unit test uses the production cache implementation and an independent OHCache implementation used to
// cross-check the production implementation.
public class CrossCheckTest
{

    private static final int fixedValueSize = 16;

    @AfterMethod(alwaysRun = true)
    public void deinit()
    {
        Uns.clearUnsDebugForTest();
    }

    static DoubleCheckCacheImpl<byte[], byte[]> cache(Eviction eviction, HashAlgorithm hashAlgorithm)
    {
        return cache(eviction, hashAlgorithm, 256);
    }

    static DoubleCheckCacheImpl<byte[], byte[]> cache(Eviction eviction, HashAlgorithm hashAlgorithm, long capacity)
    {
        return cache(eviction, hashAlgorithm, capacity, -1);
    }

    static DoubleCheckCacheImpl<byte[], byte[]> cache(Eviction eviction, HashAlgorithm hashAlgorithm, long capacity, int hashTableSize)
    {
        return cache(eviction, hashAlgorithm, capacity, hashTableSize, -1, -1);
    }

    static DoubleCheckCacheImpl<byte[], byte[]> cache(Eviction eviction, HashAlgorithm hashAlgorithm, long capacity, int hashTableSize, int segments, long maxEntrySize)
    {
        OHCacheBuilder<byte[], byte[]> builder = OHCacheBuilder.<byte[], byte[]>newBuilder()
                                                                .keySerializer(new ByteArraySerializer())
                                                                .valueSerializer(new ByteArraySerializer())
                                                                .hashMode(hashAlgorithm)
                                                                .fixedValueSize(fixedValueSize)
                                                                .capacity(capacity * TestUtils.ONE_MB);
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

    @DataProvider(name = "types")
    public Object[][] cacheEviction()
    {
        return new Object[][]{
        { Eviction.NONE, HashAlgorithm.MURMUR3 },
        { Eviction.NONE, HashAlgorithm.CRC32 },
        { Eviction.NONE, HashAlgorithm.XX }
        };
    }

    @Test(dataProvider = "types")
    public void testBasics(Eviction eviction, HashAlgorithm hashAlgorithm) throws IOException, InterruptedException
    {
        try (OHCache<byte[], byte[]> cache = cache(eviction, hashAlgorithm))
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

    @Test(dataProvider = "types", dependsOnMethods = "testBasics")
    public void testManyValues(Eviction eviction, HashAlgorithm hashAlgorithm) throws IOException, InterruptedException
    {
        try (OHCache<byte[], byte[]> cache = cache(eviction, hashAlgorithm, 64, -1))
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
    @Test(dataProvider = "types", dependsOnMethods = "testBasics",
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = ".*greater than fixed value size.*")
    public void testPutTooLargeValue(Eviction eviction, HashAlgorithm hashAlgorithm) throws IOException, InterruptedException {
        byte[] key = TestUtils.randomBytes(8);
        byte[] largeValue = TestUtils.randomBytes(fixedValueSize + 1);

        try (OHCache<byte[], byte[]> cache = cache(eviction, hashAlgorithm, 1, -1)) {
            cache.put(key, largeValue);
        }
    }

    @Test(dataProvider = "types", dependsOnMethods = "testBasics",
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = ".*exceeds max permitted size of 127")
    public void testPutTooLargeKey(Eviction eviction, HashAlgorithm hashAlgorithm) throws IOException, InterruptedException {
        byte[] key = TestUtils.randomBytes(1024);
        byte[] largeValue = TestUtils.randomBytes(fixedValueSize);

        try (OHCache<byte[], byte[]> cache = cache(eviction, hashAlgorithm, 1, -1)) {
            cache.put(key, largeValue);
        }
    }

    // per-method tests

    @Test(dataProvider = "types", dependsOnMethods = "testBasics")
    public void testAddOrReplace(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (OHCache<byte[], byte[]> cache = cache(eviction, hashAlgorithm))
        {
            for (int i = 0; i < TestUtils.manyCount; i++)
                assertTrue(cache.addOrReplace(Longs.toByteArray(i), TestUtils.randomBytes(fixedValueSize), TestUtils.randomBytes(fixedValueSize)));

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
        }
    }

    @Test(dataProvider = "types")
    public void testPutIfAbsent(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (OHCache<byte[], byte[]> cache = cache(eviction, hashAlgorithm))
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

    @Test(dataProvider = "types")
    public void testPutAll(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (OHCache<byte[], byte[]> cache = cache(eviction, hashAlgorithm))
        {
            Map<byte[], byte[]> map = new HashMap<>();
            map.put(Longs.toByteArray(1), TestUtils.randomBytes(fixedValueSize));
            map.put(Longs.toByteArray(2), TestUtils.randomBytes(fixedValueSize));
            map.put(Longs.toByteArray(3), TestUtils.randomBytes(fixedValueSize));
            cache.putAll(map);

            map.forEach((key, value) -> {
                Assert.assertEquals(cache.get(key), value);
            });
        }
    }

    @Test(dataProvider = "types")
    public void testRemove(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (OHCache<byte[], byte[]> cache = cache(eviction, hashAlgorithm))
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

    @Test(dataProvider = "types")
    public void testRemoveAll(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (OHCache<byte[], byte[]> cache = cache(eviction, hashAlgorithm))
        {
            List<TestUtils.KeyValuePair> data = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                data.add(new TestUtils.KeyValuePair(Longs.toByteArray(i), TestUtils.randomBytes(fixedValueSize)));
            }

            data.forEach(kv -> cache.put(kv.key, kv.value));
            data.forEach(kv -> Assert.assertEquals(cache.get(kv.key), kv.value));

            Assert.assertEquals(cache.size(), 100);

            // remove first 10 elements.
            cache.removeAll(data.stream().limit(10).map(kv -> kv.key).collect(Collectors.toList()));

            Assert.assertEquals(cache.size(), 90);


            // remove next 40 elements.
            cache.removeAll(data.stream().skip(10).limit(40).map(kv -> kv.key).collect(Collectors.toList()));

            Assert.assertEquals(cache.size(), 50);

            // remove rest of the elements
            cache.removeAll(data.stream().skip(50).map(kv -> kv.key).collect(Collectors.toList()));
            Assert.assertEquals(cache.size(), 0);
        }
    }

    @Test(dataProvider = "types")
    public void testClear(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (OHCache<byte[], byte[]> cache = cache(eviction, hashAlgorithm))
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

    @Test(dataProvider = "types")
    public void testGet_Put(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (OHCache<byte[], byte[]> cache = cache(eviction, hashAlgorithm))
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

    @Test(dataProvider = "types")
    public void testContainsKey(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (OHCache<byte[], byte[]> cache = cache(eviction, hashAlgorithm))
        {
            byte[] key = Longs.toByteArray(42);
            byte[] value = TestUtils.randomBytes(fixedValueSize);
            cache.put(key, value);
            assertTrue(cache.containsKey(key));
            Assert.assertFalse(cache.containsKey(Longs.toByteArray(11)));
        }
    }

    @Test(dataProvider = "types", dependsOnMethods = "testBasics")
    public void testKeyIterator1(Eviction eviction, HashAlgorithm hashAlgorithm) throws IOException, InterruptedException
    {
        try (OHCache<byte[], byte[]> cache = cache(eviction, hashAlgorithm, 32))
        {

            List<TestUtils.KeyValuePair> data = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                data.add(new TestUtils.KeyValuePair(Longs.toByteArray(i), TestUtils.randomBytes(fixedValueSize)));
            }

            data.forEach(kv -> cache.put(kv.key, kv.value));

            Set<ByteBuffer> returned = new TreeSet<>();
            Iterator<byte[]> iter = cache.keyIterator();
            for (int i = 0; i < 100; i++)
            {
                assertTrue(iter.hasNext());
                returned.add(ByteBuffer.wrap(iter.next()));
            }
            assertFalse(iter.hasNext());
            assertEquals(returned.size(), 100);

            data.forEach(kv -> assertTrue(returned.contains(ByteBuffer.wrap(kv.key))));

            returned.clear();

            iter = cache.keyIterator();
            for (int i = 0; i < 100; i++)
            {
                assertTrue(iter.hasNext());
                returned.add(ByteBuffer.wrap(iter.next()));
            }
            assertFalse(iter.hasNext());
            assertEquals(returned.size(), 100);

            data.forEach(kv -> assertTrue(returned.contains(ByteBuffer.wrap(kv.key))));

            iter = cache.keyIterator();
            for (int i = 0; i < 100; i++)
            {
                iter.next();
                iter.remove();
            }

            Assert.assertEquals(cache.size(), 0);
            data.forEach(kv -> assertNull(cache.get(kv.key)));
        }
    }

    @Test(dataProvider = "types")
    public void testKeyIterator2(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (OHCache<byte[], byte[]> cache = cache(eviction, hashAlgorithm))
        {
            Assert.assertFalse(cache.keyIterator().hasNext());

            List<TestUtils.KeyValuePair> data = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                data.add(new TestUtils.KeyValuePair(Longs.toByteArray(i), TestUtils.randomBytes(fixedValueSize)));
            }

            data.forEach(kv -> cache.put(kv.key, kv.value));

            Assert.assertEquals(cache.stats().getSize(), 100);

            Set<ByteBuffer> keys = new TreeSet<>();
            try (CloseableIterator<byte[]> iter = cache.keyIterator())
            {
                while (iter.hasNext())
                {
                    byte[] k = iter.next();
                    assertTrue(keys.add(ByteBuffer.wrap(k)));
                }
            }

            Assert.assertEquals(keys.size(), 100);
            data.forEach(kv -> assertTrue(keys.contains(ByteBuffer.wrap(kv.key))));

            cache.clear();

            Assert.assertEquals(cache.stats().getSize(), 0);

            // add again and remove via iterator

            Assert.assertFalse(cache.keyBufferIterator().hasNext());
            data.forEach(kv -> cache.put(kv.key, kv.value));

            try (CloseableIterator<ByteBuffer> iter = cache.keyBufferIterator())
            {
                while (iter.hasNext())
                {
                    iter.next();
                    iter.remove();
                }
            }

            Assert.assertFalse(cache.keyBufferIterator().hasNext());

            Assert.assertEquals(cache.stats().getSize(), 0);
        }
    }
//
    @Test(dataProvider = "types")
    public void testKeyBufferIterator(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (OHCache<byte[], byte[]> cache = cache(eviction, hashAlgorithm))
        {
            Assert.assertFalse(cache.keyBufferIterator().hasNext());

            List<TestUtils.KeyValuePair> data = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                data.add(new TestUtils.KeyValuePair(Longs.toByteArray(i), TestUtils.randomBytes(fixedValueSize)));
            }

            data.forEach(kv -> cache.put(kv.key, kv.value));

            Assert.assertEquals(cache.stats().getSize(), 100);

            Set<ByteBuffer> keys = new HashSet<>();
            try (CloseableIterator<ByteBuffer> iter = cache.keyBufferIterator())
            {
                while (iter.hasNext())
                {
                    ByteBuffer k = iter.next();
                    ByteBuffer k2 = ByteBuffer.allocate(k.remaining());
                    k2.put(k);
                    k2.flip();
                    assertTrue(keys.add(k2));
                }
            }
            Assert.assertEquals(keys.size(), 100);
            data.forEach(kv -> assertTrue(keys.contains(ByteBuffer.wrap(kv.key))));

            cache.clear();

            Assert.assertEquals(cache.stats().getSize(), 0);

            // add again and remove via iterator

            Assert.assertFalse(cache.keyBufferIterator().hasNext());
            data.forEach(kv -> cache.put(kv.key, kv.value));

            try (CloseableIterator<ByteBuffer> iter = cache.keyBufferIterator())
            {
                while (iter.hasNext())
                {
                    iter.next();
                    iter.remove();
                }
            }

            Assert.assertFalse(cache.keyBufferIterator().hasNext());
            Assert.assertEquals(cache.stats().getSize(), 0);
        }
    }

    @Test(dataProvider = "types")
    public void testGetBucketHistogram(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (DoubleCheckCacheImpl<byte[], byte[]> cache = cache(eviction, hashAlgorithm))
        {
            Assert.assertFalse(cache.keyIterator().hasNext());
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

    @Test(dataProvider = "types")
    public void testResetStatistics(Eviction eviction, HashAlgorithm hashAlgorithm) throws IOException
    {
        try (OHCache<byte[], byte[]> cache = cache(eviction, hashAlgorithm))
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
