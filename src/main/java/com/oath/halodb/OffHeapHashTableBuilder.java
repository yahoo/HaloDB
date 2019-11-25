/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

class OffHeapHashTableBuilder<E extends HashEntry> {

    private int segmentCount;
    private int hashTableSize = 8192;
    private int memoryPoolChunkSize = 2 * 1024 * 1024;
    private final HashEntrySerializer<E> serializer;
    private float loadFactor = .75f;
    private int fixedKeySize = -1;
    private HashAlgorithm hashAlgorighm = HashAlgorithm.MURMUR3;
    private Hasher hasher;
    private boolean unlocked;
    private boolean useMemoryPool = false;

    private OffHeapHashTableBuilder(HashEntrySerializer<E> serializer) {
        this.serializer = serializer;
        int cpus = Runtime.getRuntime().availableProcessors();

        segmentCount = roundUpToPowerOf2(cpus * 2, 1 << 30);
    }

    static final String SYSTEM_PROPERTY_PREFIX = "org.caffinitas.ohc.";

    static int roundUpToPowerOf2(int number, int max) {
        return number >= max
               ? max
               : (number > 1) ? Integer.highestOneBit((number - 1) << 1) : 1;
    }

    static <E extends HashEntry> OffHeapHashTableBuilder<E> newBuilder(HashEntrySerializer<E> serializer) {
        return new OffHeapHashTableBuilder<>(serializer);
    }

    public OffHeapHashTable<E> build() {
        //TODO: write a test.
        if (useMemoryPool && fixedKeySize == -1) {
            throw new IllegalArgumentException("Need to set fixedKeySize when using memory pool");
        }

        if (serializer == null) {
            throw new IllegalArgumentException("Value serializer must be set.");
        }

        return new OffHeapHashTableImpl<>(this);
    }

    public int getHashTableSize() {
        return hashTableSize;
    }

    public OffHeapHashTableBuilder<E> hashTableSize(int hashTableSize) {
        if (hashTableSize < -1) {
            throw new IllegalArgumentException("hashTableSize:" + hashTableSize);
        }
        this.hashTableSize = hashTableSize;
        return this;
    }

    public int getMemoryPoolChunkSize() {
        return memoryPoolChunkSize;
    }

    public OffHeapHashTableBuilder<E> memoryPoolChunkSize(int chunkSize) {
        if (chunkSize < -1) {
            throw new IllegalArgumentException("memoryPoolChunkSize:" + chunkSize);
        }
        this.memoryPoolChunkSize = chunkSize;
        return this;
    }

    public HashEntrySerializer<E> getEntrySerializer() {
        return serializer;
    }

    public int getSegmentCount() {
        return segmentCount;
    }

    public OffHeapHashTableBuilder<E> segmentCount(int segmentCount) {
        if (segmentCount < -1) {
            throw new IllegalArgumentException("segmentCount:" + segmentCount);
        }
        this.segmentCount = segmentCount;
        return this;
    }

    public float getLoadFactor() {
        return loadFactor;
    }

    public OffHeapHashTableBuilder<E> loadFactor(float loadFactor) {
        if (loadFactor <= 0f) {
            throw new IllegalArgumentException("loadFactor:" + loadFactor);
        }
        this.loadFactor = loadFactor;
        return this;
    }

    public int getFixedKeySize() {
        return fixedKeySize;
    }

    public OffHeapHashTableBuilder<E> fixedKeySize(int fixedKeySize) {
        if (fixedKeySize <= 0) {
            throw new IllegalArgumentException("fixedValueSize:" + fixedKeySize);
        }
        this.fixedKeySize = fixedKeySize;
        return this;
    }

    public HashAlgorithm getHashAlgorighm() {
        return hashAlgorighm;
    }

    public Hasher getHasher() {
        return hasher;
    }

    public OffHeapHashTableBuilder<E> hashMode(HashAlgorithm hashMode) {
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

    public OffHeapHashTableBuilder<E> unlocked(boolean unlocked) {
        this.unlocked = unlocked;
        return this;
    }

    public boolean isUseMemoryPool() {
        return useMemoryPool;
    }

    public OffHeapHashTableBuilder<E> useMemoryPool(boolean useMemoryPool) {
        this.useMemoryPool = useMemoryPool;
        return this;
    }
}
