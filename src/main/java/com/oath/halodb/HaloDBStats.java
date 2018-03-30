package com.oath.halodb;

import com.google.common.base.MoreObjects;

import java.util.Arrays;
import java.util.Map;

/**
 * @author Arjun Mannaly
 */
public class HaloDBStats {

    private final long statsResetTime;

    private final long size;

    private final int numberOfFilesPendingCompaction;
    private final Map<Integer, Double> staleDataPercentPerFile;

    private final long rehashCount;
    private final long numberOfSegments;
    private final long[] countPerSegment;
    private final long maxSizePerSegment;

    private final long numberOfRecordsCopied;
    private final long numberOfRecordsReplaced;
    private final long numberOfRecordsScanned;
    private final long sizeOfRecordsCopied;
    private final long sizeOfFilesDeleted;
    private final long sizeReclaimed;

    private final HaloDBOptions options;

    public HaloDBStats(long statsResetTime, long size, int numberOfFilesPendingCompaction,
                       Map<Integer, Double> staleDataPercentPerFile, long rehashCount, long numberOfSegments,
                       long[] countPerSegment, long maxSizePerSegment, long numberOfRecordsCopied,
                       long numberOfRecordsReplaced, long numberOfRecordsScanned, long sizeOfRecordsCopied,
                       long sizeOfFilesDeleted, long sizeReclaimed, HaloDBOptions options) {
        this.statsResetTime = statsResetTime;
        this.size = size;
        this.numberOfFilesPendingCompaction = numberOfFilesPendingCompaction;
        this.staleDataPercentPerFile = staleDataPercentPerFile;
        this.rehashCount = rehashCount;
        this.numberOfSegments = numberOfSegments;
        this.countPerSegment = countPerSegment;
        this.maxSizePerSegment = maxSizePerSegment;
        this.numberOfRecordsCopied = numberOfRecordsCopied;
        this.numberOfRecordsReplaced = numberOfRecordsReplaced;
        this.numberOfRecordsScanned = numberOfRecordsScanned;
        this.sizeOfRecordsCopied = sizeOfRecordsCopied;
        this.sizeOfFilesDeleted = sizeOfFilesDeleted;
        this.sizeReclaimed = sizeReclaimed;
        this.options = options;
    }

    public long getSize() {
        return size;
    }

    public int getNumberOfFilesPendingCompaction() {
        return numberOfFilesPendingCompaction;
    }

    public Map<Integer, Double> getStaleDataPercentPerFile() {
        return staleDataPercentPerFile;
    }

    public long getRehashCount() {
        return rehashCount;
    }

    public long getNumberOfSegments() {
        return numberOfSegments;
    }

    public long[] getCountPerSegment() {
        return countPerSegment;
    }

    public long getMaxSizePerSegment() {
        return maxSizePerSegment;
    }

    public long getNumberOfRecordsCopied() {
        return numberOfRecordsCopied;
    }

    public long getNumberOfRecordsReplaced() {
        return numberOfRecordsReplaced;
    }

    public long getNumberOfRecordsScanned() {
        return numberOfRecordsScanned;
    }

    public long getSizeOfRecordsCopied() {
        return sizeOfRecordsCopied;
    }

    public long getSizeOfFilesDeleted() {
        return sizeOfFilesDeleted;
    }

    public long getSizeReclaimed() {
        return sizeReclaimed;
    }

    public HaloDBOptions getOptions() {
        return options;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("statsResetTime", statsResetTime)
            .add("size", size)
            .add("numberOfFilesPendingCompaction", numberOfFilesPendingCompaction)
            .add("numberOfRecordsCopied", numberOfRecordsCopied)
            .add("numberOfRecordsReplaced", numberOfRecordsReplaced)
            .add("numberOfRecordsScanned", numberOfRecordsScanned)
            .add("sizeOfRecordsCopied", sizeOfRecordsCopied)
            .add("sizeOfFilesDeleted", sizeOfFilesDeleted)
            .add("sizeReclaimed", sizeReclaimed)
            .add("rehashCount", rehashCount)
            .add("numberOfSegments", numberOfSegments)
            .add("countPerSegment", Arrays.toString(countPerSegment))
            .add("maxSizePerSegment", maxSizePerSegment)
            .add("Options", options)
            .add("staleDataPercentPerFile", staleDataMapToString())
            .toString();
    }

    private String staleDataMapToString() {
        StringBuilder builder = new StringBuilder("[");
        boolean isFirst = true;

        for (Map.Entry<Integer, Double> e : staleDataPercentPerFile.entrySet()) {
            if (!isFirst) {
                builder.append(", ");
            }
            isFirst = false;
            builder.append("{");
            builder.append(e.getKey());
            builder.append("=");
            builder.append(String.format("%.1f", e.getValue()));
            builder.append("}");
        }
        builder.append("]");
        return builder.toString();
    }

}
