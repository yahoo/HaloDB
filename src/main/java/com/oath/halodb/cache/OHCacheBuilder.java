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

package com.oath.halodb.cache;

import com.oath.halodb.cache.linked.OHCacheLinkedImpl;

import java.util.concurrent.ScheduledExecutorService;

public class OHCacheBuilder<K, V>
{
    private int segmentCount;
    private int hashTableSize = 8192;
    private long capacity;
    private int chunkSize;
    private CacheSerializer<K> keySerializer;
    private CacheSerializer<V> valueSerializer;
    private float loadFactor = .75f;
    private int fixedKeySize;
    private int fixedValueSize = -1;
    private long maxEntrySize;
    private ScheduledExecutorService executorService;
    private boolean throwOOME;
    private HashAlgorithm hashAlgorighm = HashAlgorithm.MURMUR3;
    private boolean unlocked;
    private long defaultTTLmillis;
    private boolean timeouts;
    private int timeoutsSlots;
    private int timeoutsPrecision;
    private int frequencySketchSize;
    private double edenSize = 0.2d;

    private OHCacheBuilder()
    {
        int cpus = Runtime.getRuntime().availableProcessors();

        segmentCount = roundUpToPowerOf2(cpus * 2, 1 << 30);
        capacity = Math.min(cpus * 16, 64) * 1024 * 1024;

        segmentCount = fromSystemProperties("segmentCount", segmentCount);
        hashTableSize = fromSystemProperties("hashTableSize", hashTableSize);
        capacity = fromSystemProperties("capacity", capacity);
        chunkSize = fromSystemProperties("chunkSize", chunkSize);
        loadFactor = fromSystemProperties("loadFactor", loadFactor);
        maxEntrySize = fromSystemProperties("maxEntrySize", maxEntrySize);
        throwOOME = fromSystemProperties("throwOOME", throwOOME);
        hashAlgorighm = HashAlgorithm.valueOf(fromSystemProperties("hashAlgorighm", hashAlgorighm.name()));
        unlocked = fromSystemProperties("unlocked", unlocked);
        defaultTTLmillis = fromSystemProperties("defaultTTLmillis", defaultTTLmillis);
        timeouts = fromSystemProperties("timeouts", timeouts);
        timeoutsSlots = fromSystemProperties("timeoutsSlots", timeoutsSlots);
        timeoutsPrecision = fromSystemProperties("timeoutsPrecision", timeoutsPrecision);
        frequencySketchSize = fromSystemProperties("frequencySketchSize", frequencySketchSize);
        edenSize = fromSystemProperties("edenSize", edenSize);
    }

    public static final String SYSTEM_PROPERTY_PREFIX = "org.caffinitas.ohc.";

    private static float fromSystemProperties(String name, float defaultValue)
    {
        try
        {
            return Float.parseFloat(System.getProperty(SYSTEM_PROPERTY_PREFIX + name, Float.toString(defaultValue)));
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to parse system property " + SYSTEM_PROPERTY_PREFIX + name, e);
        }
    }

    private static long fromSystemProperties(String name, long defaultValue)
    {
        try
        {
            return Long.parseLong(System.getProperty(SYSTEM_PROPERTY_PREFIX + name, Long.toString(defaultValue)));
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to parse system property " + SYSTEM_PROPERTY_PREFIX + name, e);
        }
    }

    private static int fromSystemProperties(String name, int defaultValue)
    {
        try
        {
            return Integer.parseInt(System.getProperty(SYSTEM_PROPERTY_PREFIX + name, Integer.toString(defaultValue)));
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to parse system property " + SYSTEM_PROPERTY_PREFIX + name, e);
        }
    }

    private static double fromSystemProperties(String name, double defaultValue)
    {
        try
        {
            return Double.parseDouble(System.getProperty(SYSTEM_PROPERTY_PREFIX + name, Double.toString(defaultValue)));
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to parse system property " + SYSTEM_PROPERTY_PREFIX + name, e);
        }
    }

    private static boolean fromSystemProperties(String name, boolean defaultValue)
    {
        try
        {
            return Boolean.parseBoolean(System.getProperty(SYSTEM_PROPERTY_PREFIX + name, Boolean.toString(defaultValue)));
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to parse system property " + SYSTEM_PROPERTY_PREFIX + name, e);
        }
    }

    private static String fromSystemProperties(String name, String defaultValue)
    {
        return System.getProperty(SYSTEM_PROPERTY_PREFIX + name, defaultValue);
    }

    private static <E extends Enum> E fromSystemProperties(String name, E defaultValue, Class<E> type)
    {
        String value = fromSystemProperties(name, defaultValue.name());
        return (E) Enum.valueOf(type, value.toUpperCase());
    }

    static int roundUpToPowerOf2(int number, int max)
    {
        return number >= max
               ? max
               : (number > 1) ? Integer.highestOneBit((number - 1) << 1) : 1;
    }

    public static <K, V> OHCacheBuilder<K, V> newBuilder()
    {
        return new OHCacheBuilder<>();
    }

    public OHCache<K, V> build()
    {
        if (fixedValueSize == -1) {
            throw new IllegalArgumentException("Need to set fixedValueSize");
        }
        return new OHCacheLinkedImpl<>(this);
    }

    public int getHashTableSize()
    {
        return hashTableSize;
    }

    public OHCacheBuilder<K, V> hashTableSize(int hashTableSize)
    {
        if (hashTableSize < -1)
            throw new IllegalArgumentException("hashTableSize:" + hashTableSize);
        this.hashTableSize = hashTableSize;
        return this;
    }

    public int getChunkSize()
    {
        return chunkSize;
    }

    public OHCacheBuilder<K, V> chunkSize(int chunkSize)
    {
        if (chunkSize < -1)
            throw new IllegalArgumentException("chunkSize:" + chunkSize);
        this.chunkSize = chunkSize;
        return this;
    }

    public long getCapacity()
    {
        return capacity;
    }

    public OHCacheBuilder<K, V> capacity(long capacity)
    {
        if (capacity <= 0)
            throw new IllegalArgumentException("capacity:" + capacity);
        this.capacity = capacity;
        return this;
    }

    public CacheSerializer<K> getKeySerializer()
    {
        return keySerializer;
    }

    public OHCacheBuilder<K, V> keySerializer(CacheSerializer<K> keySerializer)
    {
        this.keySerializer = keySerializer;
        return this;
    }

    public CacheSerializer<V> getValueSerializer()
    {
        return valueSerializer;
    }

    public OHCacheBuilder<K, V> valueSerializer(CacheSerializer<V> valueSerializer)
    {
        this.valueSerializer = valueSerializer;
        return this;
    }

    public int getSegmentCount()
    {
        return segmentCount;
    }

    public OHCacheBuilder<K, V> segmentCount(int segmentCount)
    {
        if (segmentCount < -1)
            throw new IllegalArgumentException("segmentCount:" + segmentCount);
        this.segmentCount = segmentCount;
        return this;
    }

    public float getLoadFactor()
    {
        return loadFactor;
    }

    public OHCacheBuilder<K, V> loadFactor(float loadFactor)
    {
        if (loadFactor <= 0f)
            throw new IllegalArgumentException("loadFactor:" + loadFactor);
        this.loadFactor = loadFactor;
        return this;
    }

    public long getMaxEntrySize()
    {
        return maxEntrySize;
    }

    public OHCacheBuilder<K, V> maxEntrySize(long maxEntrySize)
    {
        if (maxEntrySize < 0)
            throw new IllegalArgumentException("maxEntrySize:" + maxEntrySize);
        this.maxEntrySize = maxEntrySize;
        return this;
    }

    public int getFixedKeySize()
    {
        return fixedKeySize;
    }

    public int getFixedValueSize()
    {
        return fixedValueSize;
    }

    public OHCacheBuilder<K, V> fixedEntrySize(int fixedKeySize, int fixedValueSize)
    {
        if ((fixedKeySize > 0 || fixedValueSize > 0) &&
            (fixedKeySize <= 0 || fixedValueSize <= 0))
            throw new IllegalArgumentException("fixedKeySize:" + fixedKeySize+",fixedValueSize:" + fixedValueSize);
        this.fixedKeySize = fixedKeySize;
        this.fixedValueSize = fixedValueSize;
        return this;
    }

    public OHCacheBuilder<K, V> fixedValueSize(int fixedValueSize)
    {
        if (fixedValueSize <= 0)
            throw new IllegalArgumentException("fixedValueSize:" + fixedValueSize);
        this.fixedValueSize = fixedValueSize;
        return this;
    }

    public ScheduledExecutorService getExecutorService()
    {
        return executorService;
    }

    public OHCacheBuilder<K, V> executorService(ScheduledExecutorService executorService)
    {
        this.executorService = executorService;
        return this;
    }

    public HashAlgorithm getHashAlgorighm()
    {
        return hashAlgorighm;
    }

    public OHCacheBuilder<K, V> hashMode(HashAlgorithm hashMode)
    {
        if (hashMode == null)
            throw new NullPointerException("hashMode");
        this.hashAlgorighm = hashMode;
        return this;
    }

    public boolean isThrowOOME()
    {
        return throwOOME;
    }

    public OHCacheBuilder<K, V> throwOOME(boolean throwOOME)
    {
        this.throwOOME = throwOOME;
        return this;
    }

    public boolean isUnlocked()
    {
        return unlocked;
    }

    public OHCacheBuilder<K, V> unlocked(boolean unlocked)
    {
        this.unlocked = unlocked;
        return this;
    }

    public long getDefaultTTLmillis()
    {
        return defaultTTLmillis;
    }

    public OHCacheBuilder<K, V> defaultTTLmillis(long defaultTTLmillis)
    {
        this.defaultTTLmillis = defaultTTLmillis;
        return this;
    }

    public boolean isTimeouts()
    {
        return timeouts;
    }

    public OHCacheBuilder<K, V> timeouts(boolean timeouts)
    {
        this.timeouts = timeouts;
        return this;
    }

    public int getTimeoutsSlots()
    {
        return timeoutsSlots;
    }

    public OHCacheBuilder<K, V> timeoutsSlots(int timeoutsSlots)
    {
        if (timeoutsSlots > 0)
            this.timeouts = true;
        this.timeoutsSlots = timeoutsSlots;
        return this;
    }

    public int getTimeoutsPrecision()
    {
        return timeoutsPrecision;
    }

    public OHCacheBuilder<K, V> timeoutsPrecision(int timeoutsPrecision)
    {
        if (timeoutsPrecision > 0)
            this.timeouts = true;
        this.timeoutsPrecision = timeoutsPrecision;
        return this;
    }

    public int getFrequencySketchSize()
    {
        return frequencySketchSize;
    }

    public OHCacheBuilder<K, V> frequencySketchSize(int frequencySketchSize)
    {
        this.frequencySketchSize = frequencySketchSize;
        return this;
    }

    public double getEdenSize()
    {
        return edenSize;
    }

    public OHCacheBuilder<K, V> edenSize(double edenSize)
    {
        this.edenSize = edenSize;
        return this;
    }
}
