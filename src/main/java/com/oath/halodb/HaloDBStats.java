/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import com.google.common.base.MoreObjects;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class HaloDBStats {

    private final long statsResetTime;

    private final long size;

    private final int numberOfFilesPendingCompaction;
    private final Map<Integer, Double> staleDataPercentPerFile;

    private final long rehashCount;
    private final long numberOfSegments;
    private final long maxSizePerSegment;
    private final SegmentStats[] segmentStats;

    private final long numberOfTombstonesFoundDuringOpen;
    private final long numberOfTombstonesCleanedUpDuringOpen;

    private final int numberOfDataFiles;
    private final int numberOfTombstoneFiles;

    private final long numberOfRecordsCopied;
    private final long numberOfRecordsReplaced;
    private final long numberOfRecordsScanned;
    private final long sizeOfRecordsCopied;
    private final long sizeOfFilesDeleted;
    private final long sizeReclaimed;

    private final long compactionRateInInternal;
    private final long compactionRateSinceBeginning;

    private final boolean isCompactionRunning;

    private final HaloDBOptions options;

    public HaloDBStats(long statsResetTime, long size, boolean isCompactionRunning, int numberOfFilesPendingCompaction,
                       Map<Integer, Double> staleDataPercentPerFile, long rehashCount, long numberOfSegments,
                       long maxSizePerSegment, SegmentStats[] segmentStats,
                       int numberOfDataFiles, int numberOfTombstoneFiles,
                       long numberOfTombstonesFoundDuringOpen, long numberOfTombstonesCleanedUpDuringOpen,
                       long numberOfRecordsCopied, long numberOfRecordsReplaced, long numberOfRecordsScanned,
                       long sizeOfRecordsCopied, long sizeOfFilesDeleted, long sizeReclaimed,
                       long compactionRateSinceBeginning, HaloDBOptions options) {
        this.statsResetTime = statsResetTime;
        this.size = size;
        this.numberOfFilesPendingCompaction = numberOfFilesPendingCompaction;
        this.staleDataPercentPerFile = staleDataPercentPerFile;
        this.rehashCount = rehashCount;
        this.numberOfSegments = numberOfSegments;
        this.maxSizePerSegment = maxSizePerSegment;
        this.segmentStats = segmentStats;
        this.numberOfDataFiles = numberOfDataFiles;
        this.numberOfTombstoneFiles = numberOfTombstoneFiles;
        this.numberOfTombstonesFoundDuringOpen = numberOfTombstonesFoundDuringOpen;
        this.numberOfTombstonesCleanedUpDuringOpen = numberOfTombstonesCleanedUpDuringOpen;
        this.numberOfRecordsCopied = numberOfRecordsCopied;
        this.numberOfRecordsReplaced = numberOfRecordsReplaced;
        this.numberOfRecordsScanned = numberOfRecordsScanned;
        this.sizeOfRecordsCopied = sizeOfRecordsCopied;
        this.sizeOfFilesDeleted = sizeOfFilesDeleted;
        this.sizeReclaimed = sizeReclaimed;
        this.compactionRateSinceBeginning = compactionRateSinceBeginning;
        this.isCompactionRunning = isCompactionRunning;

        long intervalTimeInSeconds = (System.currentTimeMillis() - statsResetTime)/1000;
        if (intervalTimeInSeconds > 0) {
            this.compactionRateInInternal = sizeOfRecordsCopied/intervalTimeInSeconds;
        }
        else {
            this.compactionRateInInternal = 0;
        }

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

    public int getNumberOfDataFiles() {
        return numberOfDataFiles;
    }

    public int getNumberOfTombstoneFiles() {
        return numberOfTombstoneFiles;
    }

    public long getNumberOfTombstonesFoundDuringOpen() {
        return numberOfTombstonesFoundDuringOpen;
    }

    public long getNumberOfTombstonesCleanedUpDuringOpen() {
        return numberOfTombstonesCleanedUpDuringOpen;
    }

    public SegmentStats[] getSegmentStats() {
        return segmentStats;
    }

    public long getCompactionRateInInternal() {
        return compactionRateInInternal;
    }

    public long getCompactionRateSinceBeginning() {
        return compactionRateSinceBeginning;
    }

    public boolean isCompactionRunning() {
        return isCompactionRunning;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper("")
            .add("statsResetTime", statsResetTime)
            .add("size", size)
            .add("Options", options)
            .add("isCompactionRunning", isCompactionRunning)
            .add("CompactionJobRateInInterval", getUnit(compactionRateInInternal))
            .add("CompactionJobRateSinceBeginning", getUnit(compactionRateSinceBeginning))
            .add("numberOfFilesPendingCompaction", numberOfFilesPendingCompaction)
            .add("numberOfRecordsCopied", numberOfRecordsCopied)
            .add("numberOfRecordsReplaced", numberOfRecordsReplaced)
            .add("numberOfRecordsScanned", numberOfRecordsScanned)
            .add("sizeOfRecordsCopied", sizeOfRecordsCopied)
            .add("sizeOfFilesDeleted", sizeOfFilesDeleted)
            .add("sizeReclaimed", sizeReclaimed)
            .add("rehashCount", rehashCount)
            .add("maxSizePerSegment", maxSizePerSegment)
            .add("numberOfDataFiles", numberOfDataFiles)
            .add("numberOfTombstoneFiles", numberOfTombstoneFiles)
            .add("numberOfTombstonesFoundDuringOpen", numberOfTombstonesFoundDuringOpen)
            .add("numberOfTombstonesCleanedUpDuringOpen", numberOfTombstonesCleanedUpDuringOpen)
            .add("segmentStats", Arrays.toString(segmentStats))
            .add("numberOfSegments", numberOfSegments)
            .add("staleDataPercentPerFile", staleDataMapToString())
            .toString();
    }

    public Map<String, String> toStringMap() {
        Map<String, String> map = new HashMap<>();
        map.put("statsResetTime", String.valueOf(statsResetTime));
        map.put("size", String.valueOf(size));
        map.put("Options", String.valueOf(options));
        map.put("isCompactionRunning", String.valueOf(isCompactionRunning));
        map.put("CompactionJobRateInInterval", String.valueOf(getUnit(compactionRateInInternal)));
        map.put("CompactionJobRateSinceBeginning", String.valueOf(getUnit(compactionRateSinceBeginning)));
        map.put("numberOfFilesPendingCompaction", String.valueOf(numberOfFilesPendingCompaction));
        map.put("numberOfRecordsCopied", String.valueOf(numberOfRecordsCopied));
        map.put("numberOfRecordsReplaced", String.valueOf(numberOfRecordsReplaced));
        map.put("numberOfRecordsScanned", String.valueOf(numberOfRecordsScanned));
        map.put("sizeOfRecordsCopied", String.valueOf(sizeOfRecordsCopied));
        map.put("sizeOfFilesDeleted", String.valueOf(sizeOfFilesDeleted));
        map.put("sizeReclaimed", String.valueOf(sizeReclaimed));
        map.put("rehashCount", String.valueOf(rehashCount));
        map.put("maxSizePerSegment", String.valueOf(maxSizePerSegment));
        map.put("numberOfDataFiles", String.valueOf(numberOfDataFiles));
        map.put("numberOfTombstoneFiles", String.valueOf(numberOfTombstoneFiles));
        map.put("numberOfTombstonesFoundDuringOpen", String.valueOf(numberOfTombstonesFoundDuringOpen));
        map.put("numberOfTombstonesCleanedUpDuringOpen", String.valueOf(numberOfTombstonesCleanedUpDuringOpen));
        map.put("segmentStats", String.valueOf(Arrays.toString(segmentStats)));
        map.put("numberOfSegments", String.valueOf(numberOfSegments));
        map.put("staleDataPercentPerFile", String.valueOf(staleDataMapToString()));

        return map;
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

    private static final String gbRateUnit = " GB/second";
    private static final String mbRateUnit = " MB/second";
    private static final String kbRateUnit = " KB/second";

    private static final long GB = 1024 * 1024 * 1024;
    private static final long MB = 1024 * 1024;
    private static final long KB = 1024;

    private String getUnit(long value) {
        long temp = value / GB;
        if (temp >= 1) {
            return temp + gbRateUnit;
        }

        temp = value / MB;
        if (temp >= 1) {
            return temp + mbRateUnit;
        }

        temp = value / KB;
        return temp + kbRateUnit;
    }

}
