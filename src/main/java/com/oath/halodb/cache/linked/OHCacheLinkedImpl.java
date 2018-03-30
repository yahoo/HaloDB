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

/*
 * Modified for HaloDB.
 */
package com.oath.halodb.cache.linked;

import com.google.common.primitives.Ints;
import com.oath.halodb.cache.CacheSerializer;
import com.oath.halodb.cache.OHCacheBuilder;
import com.oath.halodb.cache.OHCacheStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ScheduledExecutorService;

import com.oath.halodb.cache.CloseableIterator;
import com.oath.halodb.cache.OHCache;
import com.oath.halodb.cache.histo.EstimatedHistogram;

public final class OHCacheLinkedImpl<K, V> implements OHCache<K, V>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OHCacheLinkedImpl.class);
    
    private final CacheSerializer<K> keySerializer;
    private final CacheSerializer<V> valueSerializer;

    private final int fixedValueLength;

    private final List<OffHeapLinkedMap<V>> maps;
    private final long segmentMask;
    private final int segmentShift;

    private final long maxEntrySize;
    private final long defaultTTL;

    private long capacity;
    private final int segmentCount;

    private volatile long putFailCount;

    private boolean closed;

    private final ScheduledExecutorService executorService;

    private final boolean throwOOME;
    private final Hasher hasher;

    public OHCacheLinkedImpl(OHCacheBuilder<K, V> builder)
    {
        long capacity = builder.getCapacity();
        if (capacity <= 0L)
            throw new IllegalArgumentException("capacity:" + capacity);

        this.capacity = capacity;

        this.defaultTTL = builder.getDefaultTTLmillis();

        this.throwOOME = builder.isThrowOOME();
        this.hasher = Hasher.create(builder.getHashAlgorighm());

        this.fixedValueLength = builder.getFixedValueSize();

        // build segments
        if (builder.getSegmentCount() <= 0)
            throw new IllegalArgumentException("Segment count should be > 0");
        segmentCount = Ints.checkedCast(Util.roundUpToPowerOf2(builder.getSegmentCount(), 1 << 30));
        maps = new ArrayList<>(segmentCount);
        for (int i = 0; i < segmentCount; i++)
        {
            try
            {
                maps.add(makeMap(builder, capacity / segmentCount));
            }
            catch (RuntimeException e)
            {
                for (;i >= 0; i--)
                    if (maps.get(i) != null)
                        maps.get(i).release();
                throw e;
            }
        }

        // bit-mask for segment part of hash
        int bitNum = Util.bitNum(segmentCount) - 1;
        this.segmentShift = 64 - bitNum;
        this.segmentMask = ((long) segmentCount - 1) << segmentShift;

        // calculate max entry size
        long maxEntrySize = builder.getMaxEntrySize();
        if (maxEntrySize > capacity / segmentCount)
            throw new IllegalArgumentException("Illegal max entry size " + maxEntrySize);
        else if (maxEntrySize <= 0)
            maxEntrySize = capacity / segmentCount;
        this.maxEntrySize = maxEntrySize;

        this.keySerializer = builder.getKeySerializer();
        if (keySerializer == null)
            throw new NullPointerException("keySerializer == null");
        this.valueSerializer = builder.getValueSerializer();
        if (valueSerializer == null)
            throw new NullPointerException("valueSerializer == null");

        this.executorService = builder.getExecutorService();

        if (LOGGER.isDebugEnabled())
            LOGGER.debug("OHC linked instance with {} segments and capacity of {} created.", segmentCount, capacity);
    }

    private OffHeapLinkedMap<V> makeMap(OHCacheBuilder<K, V> builder, long perMapCapacity) {
        return new OffHeapLinkedMap<>(builder);
    }

    //
    // map stuff
    //

    public V get(K key)
    {
        if (key == null)
            throw new NullPointerException();

        KeyBuffer keySource = keySource(key);
        return segment(keySource.hash()).getEntry(keySource);
    }

    public boolean containsKey(K key)
    {
        if (key == null)
            throw new NullPointerException();

        KeyBuffer keySource = keySource(key);
        return segment(keySource.hash()).containsEntry(keySource);
    }

    public boolean put(K k, V v)
    {
        return putInternal(k, v, false, null);
    }

    public boolean put(K k, V v, long expireAt)
    {
        return putInternal(k, v, false, null);
    }

    public boolean addOrReplace(K key, V old, V value)
    {
        return putInternal(key, value, false, old);
    }

    public boolean putIfAbsent(K k, V v)
    {
        return putInternal(k, v, true, null);
    }

    public boolean putIfAbsent(K k, V v, long expireAt)
    {
        return putInternal(k, v, true, null);
    }

    private boolean putInternal(K k, V v, boolean ifAbsent, V old)
    {
        if (k == null || v == null)
            throw new NullPointerException();

        int valueSize = valueSize(v);
        if (valueSize != fixedValueLength) {
            throw new IllegalArgumentException("value size " + valueSize + " greater than fixed value size " + fixedValueLength);
        }

        if (old != null && valueSize(old) != fixedValueLength) {
            throw new IllegalArgumentException("old value size " + valueSize(old) + " greater than fixed value size " + fixedValueLength);
        }

        int keyLen = keySize(k);
        if (keyLen > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("key size of " + keyLen + " exceeds max permitted size of " + Byte.MAX_VALUE);
        }

        long bytes = Util.allocLen(keyLen, valueSize);

        long oldValueAdr = 0L;

        try
        {
            if (old != null)
            {
                oldValueAdr = Uns.allocate(valueSize, throwOOME);
                if (oldValueAdr == 0L)
                    throw new RuntimeException("Unable to allocate " + fixedValueLength + " bytes in off-heap");
                valueSerializer.serialize(old,  Uns.directBufferFor(oldValueAdr, 0, valueSize, false));
            }

            long hashEntryAdr;
            if ((maxEntrySize > 0L && bytes > maxEntrySize) || (hashEntryAdr = Uns.allocate(bytes, throwOOME)) == 0L)
            {
                // entry too large to be inserted or OS is not able to provide enough memory
                putFailCount++;

                remove(k);

                return false;
            }

            long hash = serializeForPut(k, v, keyLen, fixedValueLength, hashEntryAdr);

            // initialize hash entry
            HashEntries.init(keyLen, hashEntryAdr);

            if (segment(hash).putEntry(hashEntryAdr, hash, keyLen, bytes, ifAbsent,
                                       oldValueAdr, fixedValueLength))
                return true;

            Uns.free(hashEntryAdr);
            return false;
        }
        finally
        {
            Uns.free(oldValueAdr);
        }
    }

    private int keySize(K k)
    {
        int sz = keySerializer.serializedSize(k);
        if (sz <= 0)
            throw new IllegalArgumentException("Illegal key length " + sz);
        return sz;
    }

    private int valueSize(V v)
    {
        int sz = valueSerializer.serializedSize(v);
        if (sz <= 0)
            throw new IllegalArgumentException("Illegal value length " + sz);
        return sz;
    }

    private long serializeForPut(K k, V v, int keyLen, long valueLen, long hashEntryAdr)
    {
        try
        {
            keySerializer.serialize(k, Uns.keyBuffer(hashEntryAdr, keyLen));
            if (v != null)
                valueSerializer.serialize(v, Uns.valueBuffer(hashEntryAdr, keyLen, valueLen));
        }
        catch (Throwable e)
        {
            freeAndThrow(e, hashEntryAdr);
        }

        return hasher.hash(hashEntryAdr, HashEntries.ENTRY_OFF_DATA, keyLen);
    }

    private static void freeAndThrow(Throwable e, long hashEntryAdr)
    {
        Uns.free(hashEntryAdr);
        if (e instanceof RuntimeException)
            throw (RuntimeException) e;
        if (e instanceof Error)
            throw (Error) e;
        throw new RuntimeException(e);
    }

    public boolean remove(K k)
    {
        if (k == null)
            throw new NullPointerException();

        KeyBuffer keySource = keySource(k);
        return segment(keySource.hash()).removeEntry(keySource);
    }

    private OffHeapLinkedMap<V> segment(long hash)
    {
        int seg = (int) ((hash & segmentMask) >>> segmentShift);
        return maps.get(seg);
    }

    private KeyBuffer keySource(K o)
    {
        int size = keySize(o);

        //TODO: here we are going to make a copy of the key
        // and compute the hash, we can avoid copy.
        KeyBuffer keyBuffer = new KeyBuffer(size);
        ByteBuffer bb = keyBuffer.byteBuffer();
        keySerializer.serialize(o, bb);
        assert(bb.position() == bb.capacity()) && (bb.capacity() == size);
        return keyBuffer.finish(hasher);
    }

    //
    // maintenance
    //

    public void clear()
    {
        for (OffHeapLinkedMap map : maps)
            map.clear();
    }

    //
    // state
    //

    //TODO: remove.
    public void setCapacity(long capacity)
    {

    }

    public void close()
    {
        closed = true;
        if (executorService != null)
            try
            {
                Thread.sleep(500);
            }
            catch (InterruptedException e)
            {
                // ignored
                Thread.currentThread().interrupt();
            }

        clear();

        for (OffHeapLinkedMap map : maps)
            map.release();
        Collections.fill(maps, null);

        if (LOGGER.isDebugEnabled())
            LOGGER.debug("Closing OHC instance");
    }

    //
    // statistics and related stuff
    //

    public void resetStatistics()
    {
        for (OffHeapLinkedMap map : maps)
            map.resetStatistics();
        putFailCount = 0;
    }

    public OHCacheStats stats()
    {
        long hitCount=0, missCount=0, evictedEntries=0, expiredEntries=0, size =0,
            freeCapacity=0, rehashes=0, putAddCount=0, putReplaceCount=0, removeCount=0;
        for (OffHeapLinkedMap map : maps) {
            hitCount += map.hitCount();
            missCount += map.missCount();
            evictedEntries += map.evictedEntries();
            expiredEntries += map.expiredEntries();
            size += map.size();
            rehashes += map.rehashes();
            putAddCount += map.putAddCount();
            putReplaceCount += map.putReplaceCount();
            removeCount += map.removeCount();
        }

        return new OHCacheStats(
                               hitCount,
                               missCount,
                               evictedEntries,
                               expiredEntries,
                               perSegmentSizes(),
                               size,
                               capacity(),
                               freeCapacity,
                               rehashes,
                               putAddCount,
                               putReplaceCount,
                               putFailCount,
                               removeCount,
                               Uns.getTotalAllocated(),
                               0L);
    }

    public long capacity()
    {
        return capacity;
    }

    //TODO: remove.
    public long freeCapacity()
    {
        long freeCapacity = 0L;
        return freeCapacity;
    }

    public long size()
    {
        long size = 0L;
        for (OffHeapLinkedMap map : maps)
            size += map.size();
        return size;
    }

    public int segments()
    {
        return maps.size();
    }

    public float loadFactor()
    {
        return maps.get(0).loadFactor();
    }

    public int[] hashTableSizes()
    {
        int[] r = new int[maps.size()];
        for (int i = 0; i < maps.size(); i++)
            r[i] = maps.get(i).hashTableSize();
        return r;
    }

    public long[] perSegmentSizes()
    {
        long[] r = new long[maps.size()];
        for (int i = 0; i < maps.size(); i++)
            r[i] = maps.get(i).size();
        return r;
    }

    public EstimatedHistogram getBucketHistogram()
    {
        EstimatedHistogram hist = new EstimatedHistogram();
        for (OffHeapLinkedMap map : maps)
            map.updateBucketHistogram(hist);

        long[] offsets = hist.getBucketOffsets();
        long[] buckets = hist.getBuckets(false);

        for (int i = buckets.length - 1; i > 0; i--)
        {
            if (buckets[i] != 0L)
            {
                offsets = Arrays.copyOf(offsets, i + 2);
                buckets = Arrays.copyOf(buckets, i + 3);
                System.arraycopy(offsets, 0, offsets, 1, i + 1);
                System.arraycopy(buckets, 0, buckets, 1, i + 2);
                offsets[0] = 0L;
                buckets[0] = 0L;
                break;
            }
        }

        for (int i = 0; i < offsets.length; i++)
            offsets[i]--;

        return new EstimatedHistogram(offsets, buckets);
    }

    //
    // convenience methods
    //

    public void putAll(Map<? extends K, ? extends V> m)
    {
        // could be improved by grouping puts by segment - but increases heap pressure and complexity - decide later
        for (Map.Entry<? extends K, ? extends V> entry : m.entrySet())
            put(entry.getKey(), entry.getValue());
    }

    public void removeAll(Iterable<K> iterable)
    {
        // could be improved by grouping removes by segment - but increases heap pressure and complexity - decide later
        for (K o : iterable)
            remove(o);
    }

    public long memUsed()
    {
        return capacity() - freeCapacity();
    }

    //
    // key iterators
    //

    public CloseableIterator<K> keyIterator()
    {
        return new AbstractKeyIterator<K>()
        {
            K buildResult(long hashEntryAdr)
            {
                return keySerializer.deserialize(Uns.keyBufferR(hashEntryAdr));
            }
        };
    }

    public CloseableIterator<ByteBuffer> keyBufferIterator()
    {
        return new AbstractKeyIterator<ByteBuffer>()
        {
            ByteBuffer buildResult(long hashEntryAdr)
            {
                return Uns.directBufferFor(hashEntryAdr, HashEntries.ENTRY_OFF_DATA, HashEntries.getKeyLen(hashEntryAdr), true);
            }
        };
    }

    private abstract class AbstractKeyIterator<R> implements CloseableIterator<R>
    {
        private int segmentIndex;
        private OffHeapLinkedMap segment;

        private int mapSegmentCount;
        private int mapSegmentIndex;

        private final LongArrayList hashEntryAdrs = new LongArrayList(1024);
        private int listIndex;

        private boolean eod;
        private R next;

        private OffHeapLinkedMap lastSegment;
        private long lastHashEntryAdr;

        public void close()
        {
        }

        public boolean hasNext()
        {
            if (eod)
                return false;

            if (next == null)
                next = computeNext();

            return next != null;
        }

        public R next()
        {
            if (eod)
                throw new NoSuchElementException();

            if (next == null)
                next = computeNext();
            R r = next;
            next = null;
            if (!eod)
                return r;
            throw new NoSuchElementException();
        }

        public void remove()
        {
        }

        private R computeNext()
        {

            while (true)
            {
                if (listIndex < hashEntryAdrs.size())
                {
                    long hashEntryAdr = hashEntryAdrs.getLong(listIndex++);
                    lastSegment = segment;
                    lastHashEntryAdr = hashEntryAdr;
                    return buildResult(hashEntryAdr);
                }

                if (mapSegmentIndex >= mapSegmentCount)
                {
                    if (segmentIndex == maps.size())
                    {
                        eod = true;
                        return null;
                    }
                    segment = maps.get(segmentIndex++);
                    mapSegmentCount = segment.hashTableSize();
                    mapSegmentIndex = 0;
                }

                if (listIndex == hashEntryAdrs.size())
                {
                    hashEntryAdrs.clear();
                    segment.getEntryAddresses(mapSegmentIndex, 1024, hashEntryAdrs);
                    mapSegmentIndex += 1024;
                    listIndex = 0;
                }
            }
        }

        abstract R buildResult(long hashEntryAdr);
    }

    public String toString()
    {
        return getClass().getSimpleName() +
               "(capacity=" + capacity() +
               " ,segments=" + maps.size() +
               " ,defaultTTL=" + defaultTTL +
               " ,maxEntrySize=" + maxEntrySize;
    }
}
