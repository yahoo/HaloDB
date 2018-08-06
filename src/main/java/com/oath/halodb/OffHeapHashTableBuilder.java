/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

class OffHeapHashTableBuilder<V> {

    private int segmentCount;
    private int hashTableSize = 8192;
    private long capacity;
    private int memoryPoolChunkSize = 2 * 1024 * 1024;
    private HashTableValueSerializer<V> valueSerializer;
    private float loadFactor = .75f;
    private int fixedKeySize = -1;
    private int fixedValueSize = -1;
    private long maxEntrySize;
    private HashAlgorithm hashAlgorighm = HashAlgorithm.MURMUR3;
    private Hasher hasher;
    private boolean unlocked;
    private boolean useMemoryPool = false;

    private OffHeapHashTableBuilder() {
        int cpus = Runtime.getRuntime().availableProcessors();

        segmentCount = roundUpToPowerOf2(cpus * 2, 1 << 30);
        capacity = Math.min(cpus * 16, 64) * 1024 * 1024;

        segmentCount = fromSystemProperties("segmentCount", segmentCount);
        hashTableSize = fromSystemProperties("hashTableSize", hashTableSize);
        capacity = fromSystemProperties("capacity", capacity);
        memoryPoolChunkSize = fromSystemProperties("memoryPoolChunkSize", memoryPoolChunkSize);
        loadFactor = fromSystemProperties("loadFactor", loadFactor);
        maxEntrySize = fromSystemProperties("maxEntrySize", maxEntrySize);
        hashAlgorighm = HashAlgorithm.valueOf(fromSystemProperties("hashAlgorighm", hashAlgorighm.name()));
        unlocked = fromSystemProperties("unlocked", unlocked);
    }

    public static final String SYSTEM_PROPERTY_PREFIX = "org.caffinitas.ohc.";

    private static float fromSystemProperties(String name, float defaultValue) {
        try {
            return Float.parseFloat(System.getProperty(SYSTEM_PROPERTY_PREFIX + name, Float.toString(defaultValue)));
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse system property " + SYSTEM_PROPERTY_PREFIX + name, e);
        }
    }

    private static long fromSystemProperties(String name, long defaultValue) {
        try {
            return Long.parseLong(System.getProperty(SYSTEM_PROPERTY_PREFIX + name, Long.toString(defaultValue)));
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse system property " + SYSTEM_PROPERTY_PREFIX + name, e);
        }
    }

    private static int fromSystemProperties(String name, int defaultValue) {
        try {
            return Integer.parseInt(System.getProperty(SYSTEM_PROPERTY_PREFIX + name, Integer.toString(defaultValue)));
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse system property " + SYSTEM_PROPERTY_PREFIX + name, e);
        }
    }

    private static double fromSystemProperties(String name, double defaultValue) {
        try {
            return Double.parseDouble(System.getProperty(SYSTEM_PROPERTY_PREFIX + name, Double.toString(defaultValue)));
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse system property " + SYSTEM_PROPERTY_PREFIX + name, e);
        }
    }

    private static boolean fromSystemProperties(String name, boolean defaultValue) {
        try {
            return Boolean
                .parseBoolean(System.getProperty(SYSTEM_PROPERTY_PREFIX + name, Boolean.toString(defaultValue)));
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse system property " + SYSTEM_PROPERTY_PREFIX + name, e);
        }
    }

    private static String fromSystemProperties(String name, String defaultValue) {
        return System.getProperty(SYSTEM_PROPERTY_PREFIX + name, defaultValue);
    }

    private static <E extends Enum> E fromSystemProperties(String name, E defaultValue, Class<E> type) {
        String value = fromSystemProperties(name, defaultValue.name());
        return (E) Enum.valueOf(type, value.toUpperCase());
    }

    static int roundUpToPowerOf2(int number, int max) {
        return number >= max
               ? max
               : (number > 1) ? Integer.highestOneBit((number - 1) << 1) : 1;
    }

    public static <V> OffHeapHashTableBuilder<V> newBuilder() {
        return new OffHeapHashTableBuilder<>();
    }

    public OffHeapHashTable<V> build() {
        if (fixedValueSize == -1) {
            throw new IllegalArgumentException("Need to set fixedValueSize");
        }

        //TODO: write a test.
        if (useMemoryPool && fixedKeySize == -1) {
            throw new IllegalArgumentException("Need to set fixedKeySize when using memory pool");
        }

        if (valueSerializer == null) {
            throw new IllegalArgumentException("Value serializer must be set.");
        }

        return new OffHeapHashTableImpl<>(this);
    }

    public int getHashTableSize() {
        return hashTableSize;
    }

    public OffHeapHashTableBuilder<V> hashTableSize(int hashTableSize) {
        if (hashTableSize < -1) {
            throw new IllegalArgumentException("hashTableSize:" + hashTableSize);
        }
        this.hashTableSize = hashTableSize;
        return this;
    }

    public int getMemoryPoolChunkSize() {
        return memoryPoolChunkSize;
    }

    public OffHeapHashTableBuilder<V> memoryPoolChunkSize(int chunkSize) {
        if (chunkSize < -1) {
            throw new IllegalArgumentException("memoryPoolChunkSize:" + chunkSize);
        }
        this.memoryPoolChunkSize = chunkSize;
        return this;
    }

    public long getCapacity() {
        return capacity;
    }

    public OffHeapHashTableBuilder<V> capacity(long capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity:" + capacity);
        }
        this.capacity = capacity;
        return this;
    }

    public HashTableValueSerializer<V> getValueSerializer() {
        return valueSerializer;
    }

    public OffHeapHashTableBuilder<V> valueSerializer(HashTableValueSerializer<V> valueSerializer) {
        this.valueSerializer = valueSerializer;
        return this;
    }

    public int getSegmentCount() {
        return segmentCount;
    }

    public OffHeapHashTableBuilder<V> segmentCount(int segmentCount) {
        if (segmentCount < -1) {
            throw new IllegalArgumentException("segmentCount:" + segmentCount);
        }
        this.segmentCount = segmentCount;
        return this;
    }

    public float getLoadFactor() {
        return loadFactor;
    }

    public OffHeapHashTableBuilder<V> loadFactor(float loadFactor) {
        if (loadFactor <= 0f) {
            throw new IllegalArgumentException("loadFactor:" + loadFactor);
        }
        this.loadFactor = loadFactor;
        return this;
    }

    public long getMaxEntrySize() {
        return maxEntrySize;
    }

    public OffHeapHashTableBuilder<V> maxEntrySize(long maxEntrySize) {
        if (maxEntrySize < 0) {
            throw new IllegalArgumentException("maxEntrySize:" + maxEntrySize);
        }
        this.maxEntrySize = maxEntrySize;
        return this;
    }

    public int getFixedKeySize() {
        return fixedKeySize;
    }

    public OffHeapHashTableBuilder<V> fixedKeySize(int fixedKeySize) {
        if (fixedKeySize <= 0) {
            throw new IllegalArgumentException("fixedValueSize:" + fixedKeySize);
        }
        this.fixedKeySize = fixedKeySize;
        return this;
    }

    public int getFixedValueSize() {
        return fixedValueSize;
    }

    public OffHeapHashTableBuilder<V> fixedEntrySize(int fixedKeySize, int fixedValueSize) {
        if ((fixedKeySize > 0 || fixedValueSize > 0) &&
            (fixedKeySize <= 0 || fixedValueSize <= 0)) {
            throw new IllegalArgumentException("fixedKeySize:" + fixedKeySize + ",fixedValueSize:" + fixedValueSize);
        }
        this.fixedKeySize = fixedKeySize;
        this.fixedValueSize = fixedValueSize;
        return this;
    }

    public OffHeapHashTableBuilder<V> fixedValueSize(int fixedValueSize) {
        if (fixedValueSize <= 0) {
            throw new IllegalArgumentException("fixedValueSize:" + fixedValueSize);
        }
        this.fixedValueSize = fixedValueSize;
        return this;
    }

    public HashAlgorithm getHashAlgorighm() {
        return hashAlgorighm;
    }

    public Hasher getHasher() {
        return hasher;
    }

    public OffHeapHashTableBuilder<V> hashMode(HashAlgorithm hashMode) {
        if (hashMode == null) {
            throw new NullPointerException("hashMode");
        }
        this.hashAlgorighm = hashMode;
        this.hasher = Hasher.create(hashMode);
        return this;
    }

    public boolean isUnlocked() {
        return unlocked;
    }

    public OffHeapHashTableBuilder<V> unlocked(boolean unlocked) {
        this.unlocked = unlocked;
        return this;
    }

    public boolean isUseMemoryPool() {
        return useMemoryPool;
    }

    public OffHeapHashTableBuilder<V> useMemoryPool(boolean useMemoryPool) {
        this.useMemoryPool = useMemoryPool;
        return this;
    }
}
