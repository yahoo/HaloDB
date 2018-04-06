package com.oath.halodb;

import com.google.common.base.MoreObjects;

/**
 * @author Arjun Mannaly
 */
public class HaloDBOptions implements Cloneable {

    //TODO; convert to private with get+set.

    // threshold of stale data at which file needs to be compacted.
    public double compactionThresholdPerFile = 0.75;

    public long maxFileSize = 1024 * 1024; /* 1mb file recordSize */


    /**
     * Data will be flushed to disk after flushDataSizeBytes have been written.
     * -1 disables explicit flushing and let the kernel handle it.
     */
    public long flushDataSizeBytes = -1;

    // used for testing.
    public boolean isCompactionDisabled = false;

    public int numberOfRecords = 1_000_000;

    // MB of data to be compacted per second.
    public int compactionJobRate = 1024 * 1024 * 1024;

    public boolean cleanUpKeyCacheOnClose = false;

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
        return MoreObjects.toStringHelper(this)
            .add("compactionThresholdPerFile", compactionThresholdPerFile)
            .add("maxFileSize", maxFileSize)
            .add("flushDataSizeBytes", flushDataSizeBytes)
            .add("isCompactionDisabled", isCompactionDisabled)
            .add("numberOfRecords", numberOfRecords)
            .add("compactionJobRate", compactionJobRate)
            .add("cleanUpKeyCacheOnClose", cleanUpKeyCacheOnClose)
            .toString();
    }
}
