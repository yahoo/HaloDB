/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

class OffHeapHashTableBuilder<V> {

    private int segmentCount;
    private int hashTableSize = 8192;
    private int memoryPoolChunkSize = 2 * 1024 * 1024;
    private HashTableValueSerializer<V> valueSerializer;
    private float loadFactor = .75f;
    private int fixedKeySize = -1;
    private int fixedValueSize = -1;
    private HashAlgorithm hashAlgorighm = HashAlgorithm.MURMUR3;
    private Hasher hasher;
    private boolean unlocked;
    private boolean useMemoryPool = false;

    private OffHeapHashTableBuilder() {
        int cpus = Runtime.getRuntime().availableProcessors();

        segmentCount = roundUpToPowerOf2(cpus * 2, 1 << 30);
    }

    static final String SYSTEM_PROPERTY_PREFIX = "org.caffinitas.ohc.";

    static int roundUpToPowerOf2(int number, int max) {
        return number >= max
               ? max
               : (number > 1) ? Integer.highestOneBit((number - 1) << 1) : 1;
    }

    static <V> OffHeapHashTableBuilder<V> newBuilder() {
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
