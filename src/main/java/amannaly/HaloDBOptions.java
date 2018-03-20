package amannaly;

/**
 * @author Arjun Mannaly
 */
public class HaloDBOptions {

    //TODO; convert to private with get+set.

    // threshold of stale data at which file needs to be compacted.
    public double mergeThresholdPerFile = 0.75;

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

}
