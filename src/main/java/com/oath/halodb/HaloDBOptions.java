/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import com.google.common.base.MoreObjects;

public class HaloDBOptions implements Cloneable {

    // threshold of stale data at which file needs to be compacted.
    private double compactionThresholdPerFile = 0.75;

    private int maxFileSize = 1024 * 1024; /* 1mb file recordSize */

    private int maxTombstoneFileSize = 0; /* use maxFileSize by default unless set explicitly */

     // Data will be flushed to disk after flushDataSizeBytes have been written.
     // -1 disables explicit flushing and let the kernel handle it.
    private long flushDataSizeBytes = -1;

    // Write call will sync data to disk before returning.
    // If enabled trades off write throughput for durability.
    private boolean syncWrite = false;

    private int numberOfRecords = 1_000_000;

    // MB of data to be compacted per second.
    private int compactionJobRate = 1024 * 1024 * 1024;

    private boolean cleanUpInMemoryIndexOnClose = false;

    private boolean cleanUpTombstonesDuringOpen = false;

    private boolean useMemoryPool = false;

    private int fixedKeySize = Byte.MAX_VALUE;

    private int memoryPoolChunkSize = 16 * 1024 * 1024;

    // Just to avoid clients having to deal with CloneNotSupportedException
    public HaloDBOptions clone() {
        try {
            return (HaloDBOptions) super.clone();
        } catch (CloneNotSupportedException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper("")
            .add("compactionThresholdPerFile", compactionThresholdPerFile)
            .add("maxFileSize", maxFileSize)
            .add("maxTombstoneFileSize", getMaxTombstoneFileSize())
            .add("flushDataSizeBytes", flushDataSizeBytes)
            .add("syncWrite", syncWrite)
            .add("numberOfRecords", numberOfRecords)
            .add("compactionJobRate", compactionJobRate)
            .add("cleanUpInMemoryIndexOnClose", cleanUpInMemoryIndexOnClose)
            .add("cleanUpTombstonesDuringOpen", cleanUpTombstonesDuringOpen)
            .add("useMemoryPool", useMemoryPool)
            .add("fixedKeySize", fixedKeySize)
            .add("memoryPoolChunkSize", memoryPoolChunkSize)
            .toString();
    }

    public void setCompactionThresholdPerFile(double compactionThresholdPerFile) {
        this.compactionThresholdPerFile = compactionThresholdPerFile;
    }

    public void setMaxFileSize(int maxFileSize) {
        if (maxFileSize <= 0) {
            throw new IllegalArgumentException("maxFileSize should be > 0");
        }
        this.maxFileSize = maxFileSize;
    }

    public void setMaxTombstoneFileSize(int maxFileSize) {
        if (maxFileSize <= 0) {
            throw new IllegalArgumentException("maxFileSize should be > 0");
        }
        this.maxTombstoneFileSize = maxFileSize;
    }

    public void setFlushDataSizeBytes(long flushDataSizeBytes) {
        this.flushDataSizeBytes = flushDataSizeBytes;
    }

    public void setNumberOfRecords(int numberOfRecords) {
        this.numberOfRecords = numberOfRecords;
    }

    public void setCompactionJobRate(int compactionJobRate) {
        this.compactionJobRate = compactionJobRate;
    }

    public void setCleanUpInMemoryIndexOnClose(boolean cleanUpInMemoryIndexOnClose) {
        this.cleanUpInMemoryIndexOnClose = cleanUpInMemoryIndexOnClose;
    }

    public double getCompactionThresholdPerFile() {
        return compactionThresholdPerFile;
    }

    public int getMaxFileSize() {
        return maxFileSize;
    }

    public int getMaxTombstoneFileSize() {
        return maxTombstoneFileSize > 0 ? maxTombstoneFileSize : maxFileSize;
    }

    public long getFlushDataSizeBytes() {
        return flushDataSizeBytes;
    }

    public int getNumberOfRecords() {
        return numberOfRecords;
    }

    public int getCompactionJobRate() {
        return compactionJobRate;
    }

    public boolean isCleanUpInMemoryIndexOnClose() {
        return cleanUpInMemoryIndexOnClose;
    }

    public boolean isCleanUpTombstonesDuringOpen() {
        return cleanUpTombstonesDuringOpen;
    }

    public void setCleanUpTombstonesDuringOpen(boolean cleanUpTombstonesDuringOpen) {
        this.cleanUpTombstonesDuringOpen = cleanUpTombstonesDuringOpen;
    }
    
    public boolean isUseMemoryPool() {
        return useMemoryPool;
    }

    public void setUseMemoryPool(boolean useMemoryPool) {
        this.useMemoryPool = useMemoryPool;
    }

    public int getFixedKeySize() {
        return fixedKeySize;
    }

    public void setFixedKeySize(int fixedKeySize) {
        this.fixedKeySize = fixedKeySize;
    }

    public int getMemoryPoolChunkSize() {
        return memoryPoolChunkSize;
    }

    public void setMemoryPoolChunkSize(int memoryPoolChunkSize) {
        this.memoryPoolChunkSize = memoryPoolChunkSize;
    }

    public boolean isSyncWrite() {
        return syncWrite;
    }

    public void enableSyncWrites(boolean syncWrites) {
        this.syncWrite = syncWrites;
    }


    // to be used only in tests.
    private boolean isCompactionDisabled = false;
    
    // not visible to outside the package.
    // to be used only in tests.
    void setCompactionDisabled(boolean compactionDisabled) {
        isCompactionDisabled = compactionDisabled;
    }
    boolean isCompactionDisabled() {
        return isCompactionDisabled;
    }
}
