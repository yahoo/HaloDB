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

import amannaly.ohc.*;
import amannaly.ohc.histo.EstimatedHistogram;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ScheduledExecutorService;

public final class OHCacheLinkedImpl<K, V> implements OHCache<K, V>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OHCacheLinkedImpl.class);

    private static final int CURRENT_FILE_VERSION = 2;

    private final CacheSerializer<K> keySerializer;
    private final CacheSerializer<V> valueSerializer;

    private final int fixedValueLength;

    private final OffHeapLinkedMap[] maps;
    private final long segmentMask;
    private final int segmentShift;

    private final long maxEntrySize;
    private final long defaultTTL;

    private long capacity;

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
        int segments = builder.getSegmentCount();
        if (segments <= 0)
            segments = Runtime.getRuntime().availableProcessors() * 2;
        segments = Ints.checkedCast(Util.roundUpToPowerOf2(segments, 1 << 30));
        maps = new OffHeapLinkedMap[segments];
        for (int i = 0; i < segments; i++)
        {
            try
            {
                maps[i] = makeMap(builder, capacity / segments);
            }
            catch (RuntimeException e)
            {
                for (;i >= 0; i--)
                    if (maps[i] != null)
                        maps[i].release();
                throw e;
            }
        }

        // bit-mask for segment part of hash
        int bitNum = Util.bitNum(segments) - 1;
        this.segmentShift = 64 - bitNum;
        this.segmentMask = ((long) segments - 1) << segmentShift;

        // calculate max entry size
        long maxEntrySize = builder.getMaxEntrySize();
        if (maxEntrySize > capacity / segments)
            throw new IllegalArgumentException("Illegal max entry size " + maxEntrySize);
        else if (maxEntrySize <= 0)
            maxEntrySize = capacity / segments;
        this.maxEntrySize = maxEntrySize;

        this.keySerializer = builder.getKeySerializer();
        if (keySerializer == null)
            throw new NullPointerException("keySerializer == null");
        this.valueSerializer = builder.getValueSerializer();
        if (valueSerializer == null)
            throw new NullPointerException("valueSerializer == null");

        this.executorService = builder.getExecutorService();

        if (LOGGER.isDebugEnabled())
            LOGGER.debug("OHC linked instance with {} segments and capacity of {} created.", segments, capacity);
    }

    private OffHeapLinkedMap makeMap(OHCacheBuilder<K, V> builder, long perMapCapacity) {
        return new OffHeapLinkedMap(builder);
    }

    //
    // map stuff
    //

    public DirectValueAccess getDirect(K key)
    {
        return getDirect(key, true);
    }

    public DirectValueAccess getDirect(K key, boolean updateLRU)
    {
        if (key == null)
            throw new NullPointerException();

        KeyBuffer keySource = keySource(key);
        long hashEntryAdr = segment(keySource.hash()).getEntry(keySource, true, updateLRU);

        if (hashEntryAdr == 0L)
            return null;

        return new DirectValueAccessImpl(hashEntryAdr, true, fixedValueLength);
    }

    public V get(K key)
    {
        if (key == null)
            throw new NullPointerException();

        long hashEntryAdr = 0L;

        KeyBuffer keySource = keySource(key);
        try
        {
            hashEntryAdr = segment(keySource.hash()).getEntry(keySource, true, true);

            if (hashEntryAdr == 0L)
                return null;

            return valueSerializer.deserialize(Uns.valueBufferR(hashEntryAdr, fixedValueLength));
        }
        finally
        {
            HashEntries.dereference(hashEntryAdr);
        }
    }

    public boolean containsKey(K key)
    {
        if (key == null)
            throw new NullPointerException();

        KeyBuffer keySource = keySource(key);
        return segment(keySource.hash()).getEntry(keySource, false, true) != 0L;
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

    public boolean addOrReplace(K key, V old, V value, long expireAt)
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
            HashEntries.init(hash, keyLen, hashEntryAdr);

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

        return hasher.hash(hashEntryAdr, Util.ENTRY_OFF_DATA, keyLen);
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

    private OffHeapLinkedMap segment(long hash)
    {
        int seg = (int) ((hash & segmentMask) >>> segmentShift);
        return maps[seg];
    }

    private KeyBuffer keySource(K o)
    {
        int size = keySize(o);

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
        Arrays.fill(maps, null);

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
        long rehashes = 0L;
        for (OffHeapLinkedMap map : maps)
            rehashes += map.rehashes();
        return new OHCacheStats(
                               hitCount(),
                               missCount(),
                               evictedEntries(),
                               expiredEntries(),
                               perSegmentSizes(),
                               size(),
                               capacity(),
                               freeCapacity(),
                               rehashes,
                               putAddCount(),
                               putReplaceCount(),
                               putFailCount,
                               removeCount(),
                               Uns.getTotalAllocated(),
                               0L);
    }

    private long putAddCount()
    {
        long putAddCount = 0L;
        for (OffHeapLinkedMap map : maps)
            putAddCount += map.putAddCount();
        return putAddCount;
    }

    private long putReplaceCount()
    {
        long putReplaceCount = 0L;
        for (OffHeapLinkedMap map : maps)
            putReplaceCount += map.putReplaceCount();
        return putReplaceCount;
    }

    private long removeCount()
    {
        long removeCount = 0L;
        for (OffHeapLinkedMap map : maps)
            removeCount += map.removeCount();
        return removeCount;
    }

    private long hitCount()
    {
        long hitCount = 0L;
        for (OffHeapLinkedMap map : maps)
            hitCount += map.hitCount();
        return hitCount;
    }

    private long missCount()
    {
        long missCount = 0L;
        for (OffHeapLinkedMap map : maps)
            missCount += map.missCount();
        return missCount;
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

    private long evictedEntries()
    {
        long evictedEntries = 0L;
        for (OffHeapLinkedMap map : maps)
            evictedEntries += map.evictedEntries();
        return evictedEntries;
    }

    private long expiredEntries()
    {
        long expiredEntries = 0L;
        for (OffHeapLinkedMap map : maps)
            expiredEntries += map.expiredEntries();
        return expiredEntries;
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
        return maps.length;
    }

    public float loadFactor()
    {
        return maps[0].loadFactor();
    }

    public int[] hashTableSizes()
    {
        int[] r = new int[maps.length];
        for (int i = 0; i < maps.length; i++)
            r[i] = maps[i].hashTableSize();
        return r;
    }

    public long[] perSegmentSizes()
    {
        long[] r = new long[maps.length];
        for (int i = 0; i < maps.length; i++)
            r[i] = maps[i].size();
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
    // serialization (serialized data cannot be ported between different CPU architectures, if endianess differs)
    //

    public CloseableIterator<K> deserializeKeys(final ReadableByteChannel channel) throws IOException
    {
        long headerAddress = Uns.allocateIOException(8, throwOOME);
        try
        {
            ByteBuffer header = Uns.directBufferFor(headerAddress, 0L, 8L, false);
            Util.readFully(channel, header);
            header.flip();
            int magic = header.getInt();
            if (magic == Util.HEADER_KEYS_WRONG)
                throw new IOException("File from instance with different CPU architecture cannot be loaded");
            if (magic == Util.HEADER_ENTRIES)
                throw new IOException("File contains entries - expected keys");
            if (magic != Util.HEADER_KEYS)
                throw new IOException("Illegal file header");
            if (header.getInt() != CURRENT_FILE_VERSION)
                throw new IOException("Illegal file version");
        }
        finally
        {
            Uns.free(headerAddress);
        }

        return new CloseableIterator<K>()
        {
            private K next;
            private boolean eod;

            private final byte[] keyLenBuf = new byte[Util.SERIALIZED_KEY_LEN_SIZE];
            private final ByteBuffer bb = ByteBuffer.wrap(keyLenBuf);

            private long bufAdr;
            private long bufLen;

            public void close()
            {
                Uns.free(bufAdr);
                bufAdr = 0L;
            }

            protected void finalize() throws Throwable
            {
                close();
                super.finalize();
            }

            public boolean hasNext()
            {
                if (eod)
                    return false;
                if (next == null)
                    checkNext();
                return next != null;
            }

            private void checkNext()
            {
                try
                {
                    bb.clear();
                    if (!Util.readFully(channel, bb))
                    {
                        eod = true;
                        return;
                    }

                    int keyLen = Uns.getIntFromByteArray(keyLenBuf, 0);
                    long totalLen = Util.ENTRY_OFF_DATA + keyLen;

                    if (bufLen < totalLen)
                    {
                        Uns.free(bufAdr);
                        bufAdr = 0L;

                        bufLen = Math.max(4096, Util.roundUpToPowerOf2(totalLen, 1 << 30));
                        bufAdr = Uns.allocateIOException(bufLen, throwOOME);
                    }

                    if (!Util.readFully(channel, Uns.directBufferFor(bufAdr, Util.ENTRY_OFF_DATA, keyLen, false)))
                    {
                        eod = true;
                        throw new EOFException();
                    }
                    HashEntries.init(0L, keyLen, bufAdr);
                    next = keySerializer.deserialize(Uns.directBufferFor( bufAdr + Util.ENTRY_OFF_DATA, 0, keyLen, true));
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }

            public K next()
            {
                if (eod)
                    throw new NoSuchElementException();

                K r = next;
                if (r == null)
                {
                    checkNext();
                    r = next;
                }
                if (r == null)
                    throw new NoSuchElementException();
                next = null;

                return r;
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public boolean deserializeEntry(ReadableByteChannel channel) throws IOException
    {
        // read hash, keyLen, valueLen
        byte[] hashKeyValueLen = new byte[Util.SERIALIZED_ENTRY_SIZE];
        ByteBuffer bb = ByteBuffer.wrap(hashKeyValueLen);
        if (!Util.readFully(channel, bb))
            return false;

        long expireAt = 0L;

        long hash = Uns.getLongFromByteArray(hashKeyValueLen,   (int) (Util.ENTRY_OFF_HASH - Util.ENTRY_OFF_HASH));
        int keyLen = Uns.getIntFromByteArray(hashKeyValueLen,   (int) (Util.ENTRY_OFF_KEY_LENGTH - Util.ENTRY_OFF_HASH));

        long kvLen = Util.roundUpTo8(keyLen) + fixedValueLength;
        long totalLen = kvLen + Util.ENTRY_OFF_DATA;
        long hashEntryAdr;
        if ((maxEntrySize > 0L && totalLen > maxEntrySize) || (hashEntryAdr = Uns.allocate(totalLen, throwOOME)) == 0L)
        {
            if (channel instanceof SeekableByteChannel)
            {
                SeekableByteChannel sc = (SeekableByteChannel) channel;
                sc.position(sc.position() + kvLen);
            }
            else
            {
                ByteBuffer tmp = ByteBuffer.allocate(8192);
                while (kvLen > 0L)
                {
                    tmp.clear();
                    if (kvLen < tmp.capacity())
                        tmp.limit(Ints.checkedCast(kvLen));
                    if (!Util.readFully(channel, tmp))
                        return false;
                    kvLen -= tmp.limit();
                }
            }
            return false;
        }

        HashEntries.init(hash, keyLen, hashEntryAdr);

        // read key + value
        if (!Util.readFully(channel, Uns.keyBuffer(hashEntryAdr, kvLen)) ||
            !segment(hash).putEntry(hashEntryAdr, hash, keyLen, totalLen, false, 0L, 0L))
        {
            Uns.free(hashEntryAdr);
            return false;
        }

        return true;
    }

    public boolean serializeEntry(K key, WritableByteChannel channel) throws IOException
    {
        KeyBuffer keySource = keySource(key);
        long hashEntryAdr = segment(keySource.hash()).getEntry(keySource, true, true);

        return hashEntryAdr != 0L && serializeEntry(channel, hashEntryAdr, fixedValueLength);
    }

    public int deserializeEntries(ReadableByteChannel channel) throws IOException
    {
        long headerAddress = Uns.allocateIOException(8, throwOOME);
        try
        {
            ByteBuffer header = Uns.directBufferFor(headerAddress, 0L, 8L, false);
            Util.readFully(channel, header);
            header.flip();
            int magic = header.getInt();
            if (magic == Util.HEADER_ENTRIES_WRONG)
                throw new IOException("File from instance with different CPU architecture cannot be loaded");
            if (magic == Util.HEADER_KEYS)
                throw new IOException("File contains keys - expected entries");
            if (magic != Util.HEADER_ENTRIES)
                throw new IOException("Illegal file header");
            if (header.getInt() != CURRENT_FILE_VERSION)
                throw new IOException("Illegal file version");
        }
        finally
        {
            Uns.free(headerAddress);
        }

        int count = 0;
        while (deserializeEntry(channel))
            count++;
        return count;
    }

    private static boolean serializeEntry(WritableByteChannel channel, long hashEntryAdr, long valueLength) throws IOException
    {
        try
        {
            int keyLen = HashEntries.getKeyLen(hashEntryAdr);

            long totalLen = Util.SERIALIZED_ENTRY_SIZE + Util.roundUpTo8(keyLen) + valueLength;

            // write hash, keyLen, valueLen + key + value
            Util.writeFully(channel, Uns.directBufferFor(hashEntryAdr, Util.ENTRY_OFF_HASH, totalLen, true));

            return true;
        }
        finally
        {
            HashEntries.dereference(hashEntryAdr);
        }
    }

    private static boolean serializeKey(WritableByteChannel channel, long hashEntryAdr) throws IOException
    {
        try
        {
            long keyLen = HashEntries.getKeyLen(hashEntryAdr);

            long totalLen = Util.SERIALIZED_KEY_LEN_SIZE + keyLen;

            // write keyLen + key
            Util.writeFully(channel, Uns.directBufferFor(hashEntryAdr, Util.ENTRY_OFF_KEY_LENGTH, totalLen, true));

            return true;
        }
        finally
        {
            HashEntries.dereference(hashEntryAdr);
        }
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
                return Uns.directBufferFor(hashEntryAdr, Util.ENTRY_OFF_DATA, HashEntries.getKeyLen(hashEntryAdr), true);
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

        private void derefLast()
        {
            if (lastHashEntryAdr != 0L)
            {
                HashEntries.dereference(lastHashEntryAdr);
                lastHashEntryAdr = 0L;
                lastSegment = null;
            }
        }

        public void close()
        {
            derefLast();

            while (listIndex < hashEntryAdrs.size())
            {
                long hashEntryAdr = hashEntryAdrs.getLong(listIndex++);
                HashEntries.dereference(hashEntryAdr);
            }
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
            if (eod)
                throw new NoSuchElementException();

            lastSegment.removeEntry(lastHashEntryAdr);
            derefLast();
        }

        private R computeNext()
        {
            derefLast();

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
                    if (segmentIndex == maps.length)
                    {
                        eod = true;
                        return null;
                    }
                    segment = maps[segmentIndex++];
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
               " ,segments=" + maps.length +
               " ,defaultTTL=" + defaultTTL +
               " ,maxEntrySize=" + maxEntrySize;
    }
}
