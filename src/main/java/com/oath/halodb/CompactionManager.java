/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import com.google.common.util.concurrent.RateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Arjun Mannaly
 */
class CompactionManager {
    private static final Logger logger = LoggerFactory.getLogger(CompactionManager.class);

    private  final HaloDBInternal dbInternal;

    private volatile boolean isRunning = true;

    private final RateLimiter compactionRateLimiter;

    private volatile HaloDBFile currentWriteFile = null;
    private int currentWriteFileOffset = 0;

    private final BlockingQueue<Integer> compactionQueue;

    private CompactionThread compactionThread;

    private volatile long numberOfRecordsCopied = 0;
    private volatile long numberOfRecordsReplaced = 0;
    private volatile long numberOfRecordsScanned = 0;
    private volatile long sizeOfRecordsCopied = 0;
    private volatile long sizeOfFilesDeleted = 0;
    private volatile long totalSizeOfRecordsCopied = 0;
    private volatile long compactionStartTime = System.currentTimeMillis();

    private static final int STOP_SIGNAL = -10101;

    CompactionManager(HaloDBInternal dbInternal) {
        this.dbInternal = dbInternal;
        this.compactionRateLimiter = RateLimiter.create(dbInternal.options.getCompactionJobRate());
        this.compactionQueue = new LinkedBlockingQueue<>();
    }

    boolean stopCompactionThread() throws IOException {
        isRunning = false;
        if (compactionThread != null) {
            try {
                // We don't want to call interrupt on compaction thread as it
                // may interrupt IO operations and leave files in an inconsistent state.
                // instead we use -10101 as a stop signal.
                compactionQueue.put(STOP_SIGNAL);
                compactionThread.join();
                if (currentWriteFile != null) {
                    currentWriteFile.flushToDisk();
                    currentWriteFile.getIndexFile().flushToDisk();
                    currentWriteFile.close();
                }
            } catch (InterruptedException e) {
                logger.error("Error while waiting for compaction thread to stop", e);
                return false;
            }
        }
        return true;
    }

    synchronized void startCompactionThread() {
        if (compactionThread == null) {
            compactionThread = new CompactionThread();
            compactionThread.start();
        }
    }

    int getCurrentWriteFileId() {
        return currentWriteFile != null ? currentWriteFile.getFileId() : -1;
    }

    boolean submitFileForCompaction(int fileId) {
        return compactionQueue.offer(fileId);
    }

    int noOfFilesPendingCompaction() {
        return compactionQueue.size();
    }

    long getNumberOfRecordsCopied() {
        return numberOfRecordsCopied;
    }

    long getNumberOfRecordsReplaced() {
        return numberOfRecordsReplaced;
    }

    long getNumberOfRecordsScanned() {
        return numberOfRecordsScanned;
    }

    long getSizeOfRecordsCopied() {
        return sizeOfRecordsCopied;
    }

    long getSizeOfFilesDeleted() {
        return sizeOfFilesDeleted;
    }

    long getCompactionJobRateSinceBeginning() {
        long timeInSeconds = (System.currentTimeMillis() - compactionStartTime)/1000;
        long rate = 0;
        if (timeInSeconds > 0) {
            rate = totalSizeOfRecordsCopied / timeInSeconds;
        }
        return rate;
    }

    void resetStats() {
        numberOfRecordsCopied = numberOfRecordsReplaced
            = numberOfRecordsScanned = sizeOfRecordsCopied = sizeOfFilesDeleted = 0;
    }

    private class CompactionThread extends Thread {

        private long unFlushedData = 0;

        CompactionThread() {
            super("CompactionThread");

            setUncaughtExceptionHandler((t, e) -> {
                logger.error("Compaction thread crashed. Creating and running another thread. ", e);
                compactionThread = null;
                if (currentWriteFile != null) {
                    try {
                        currentWriteFile.flushToDisk();
                    } catch (IOException e1) {
                        logger.error("Error while flushing " + currentWriteFile.getFileId() + " to disk", e);
                    }
                    currentWriteFile = null;
                }
                currentWriteFileOffset = 0;
                startCompactionThread();
            });
        }

        @Override
        public void run() {
            logger.info("Starting compaction thread ...");
            int fileToCompact = -1;

            while (isRunning && !dbInternal.options.isCompactionDisabled()) {
                try {
                    fileToCompact = compactionQueue.take();
                    if (fileToCompact == STOP_SIGNAL) {
                        logger.debug("Received a stop signal.");
                        break;
                    }
                    logger.debug("Compacting {} ...", fileToCompact);
                    copyFreshRecordsToMergedFile(fileToCompact);
                    logger.debug("Completed compacting {} to {}", fileToCompact, getCurrentWriteFileId());
                    dbInternal.markFileAsCompacted(fileToCompact);
                    dbInternal.deleteHaloDBFile(fileToCompact);
                }
                catch (Exception e) {
                    logger.error(String.format("Error while compacting file %d to %d", fileToCompact, getCurrentWriteFileId()), e);
                }
            }

            logger.info("Compaction thread stopped.");
        }

        // TODO: group and move adjacent fresh records together for performance.
        private void copyFreshRecordsToMergedFile(int idOfFileToCompact) throws IOException {
            HaloDBFile fileToCompact = dbInternal.getHaloDBFile(idOfFileToCompact);
            if (fileToCompact == null) {
                logger.debug("File doesn't exist, was probably compacted already.");
                return;
            }

            FileChannel readFrom =  fileToCompact.getChannel();
            IndexFile.IndexFileIterator iterator = fileToCompact.getIndexFile().newIterator();
            long recordsCopied = 0, recordsScanned = 0;

            while (iterator.hasNext()) {
                IndexFileEntry indexFileEntry = iterator.next();
                byte[] key = indexFileEntry.getKey();
                long recordOffset = indexFileEntry.getRecordOffset();
                int recordSize = indexFileEntry.getRecordSize();
                recordsScanned++;

                InMemoryIndexMetaData currentRecordMetaData = dbInternal.getInMemoryIndex().get(key);

                if (isRecordFresh(indexFileEntry, currentRecordMetaData, idOfFileToCompact)) {
                    recordsCopied++;
                    compactionRateLimiter.acquire(recordSize);
                    rollOverCurrentWriteFile(recordSize);
                    sizeOfRecordsCopied += recordSize;
                    totalSizeOfRecordsCopied += recordSize;

                    // fresh record, copy to merged file.
                    long transferred = readFrom.transferTo(recordOffset, recordSize, currentWriteFile.getChannel());

                    //TODO: for testing. remove.
                    if (transferred != recordSize) {
                        logger.error("Had to transfer {} but only did {}", recordSize, transferred);
                    }

                    unFlushedData += transferred;
                    if (dbInternal.options.getFlushDataSizeBytes() != -1 &&
                        unFlushedData > dbInternal.options.getFlushDataSizeBytes()) {
                        currentWriteFile.getChannel().force(false);
                        unFlushedData = 0;
                    }

                    IndexFileEntry newEntry = new IndexFileEntry(
                        key, recordSize, currentWriteFileOffset,
                        indexFileEntry.getSequenceNumber(), indexFileEntry.getVersion(), -1
                    );
                    currentWriteFile.getIndexFile().write(newEntry);

                    int valueOffset = Utils.getValueOffset(currentWriteFileOffset, key);
                    InMemoryIndexMetaData newMetaData = new InMemoryIndexMetaData(
                        currentWriteFile.getFileId(), valueOffset,
                        currentRecordMetaData.getValueSize(), indexFileEntry.getSequenceNumber()
                    );

                    boolean updated = dbInternal.getInMemoryIndex().replace(key, currentRecordMetaData, newMetaData);
                    if (updated) {
                        numberOfRecordsReplaced++;
                    }
                    else {
                        // write thread wrote a new version while this version was being compacted.
                        // therefore, this version is stale.
                        dbInternal.addFileToCompactionQueueIfThresholdCrossed(currentWriteFile.getFileId(), recordSize);
                    }
                    currentWriteFileOffset += recordSize;
                    currentWriteFile.setWriteOffset(currentWriteFileOffset);
                }
            }

            if (recordsCopied > 0) {
                // After compaction we will delete the stale file.
                // To prevent data loss in the event of a crash we need to ensure that copied data has hit the disk.
                currentWriteFile.flushToDisk();
            }

            numberOfRecordsCopied += recordsCopied;
            numberOfRecordsScanned += recordsScanned;
            sizeOfFilesDeleted += fileToCompact.getSize();

            logger.debug("Scanned {} records in file {} and copied {} records to {}.datac", recordsScanned, idOfFileToCompact, recordsCopied, getCurrentWriteFileId());
        }

        private boolean isRecordFresh(IndexFileEntry entry, InMemoryIndexMetaData metaData, int idOfFileToMerge) {
            return metaData != null
                   && metaData.getFileId() == idOfFileToMerge
                   && metaData.getValueOffset() == Utils.getValueOffset(entry.getRecordOffset(), entry.getKey());
        }

        private void rollOverCurrentWriteFile(int recordSize) throws IOException {
            if (currentWriteFile == null ||  currentWriteFileOffset + recordSize > dbInternal.options.getMaxFileSize()) {
                if (currentWriteFile != null) {
                    currentWriteFile.flushToDisk();
                    currentWriteFile.getIndexFile().flushToDisk();
                }
                currentWriteFile = dbInternal.createHaloDBFile(HaloDBFile.FileType.COMPACTED_FILE);
                dbInternal.getDbDirectory().syncMetaData();
                currentWriteFileOffset = 0;
            }
        }
    }


    // Used only for tests. to be called only after all writes in the test have been performed.  
    boolean isCompactionComplete() {
        if (dbInternal.options.isCompactionDisabled())
            return true;

        if (compactionQueue.isEmpty()) {
            try {
                submitFileForCompaction(STOP_SIGNAL);
                compactionThread.join();
            } catch (InterruptedException e) {
                logger.error("Error in isCompactionComplete", e);
            }

            return true;
        }

        return false;
    }
}
