/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.RateLimiter;

class CompactionManager {
    private static final Logger logger = LoggerFactory.getLogger(CompactionManager.class);

    private  final HaloDBInternal dbInternal;

    private volatile boolean isRunning = false;

    private final RateLimiter compactionRateLimiter;

    private volatile HaloDBFile currentWriteFile = null;
    private int currentWriteFileOffset = 0;

    private final BlockingQueue<Integer> compactionQueue;

    private volatile CompactionThread compactionThread;

    private volatile long numberOfRecordsCopied = 0;
    private volatile long numberOfRecordsReplaced = 0;
    private volatile long numberOfRecordsScanned = 0;
    private volatile long sizeOfRecordsCopied = 0;
    private volatile long sizeOfFilesDeleted = 0;
    private volatile long totalSizeOfRecordsCopied = 0;
    private volatile long compactionStartTime = System.currentTimeMillis();

    // These are purposely 'newed' up because we use reference equality to check the signals and the value does not matter
    // signal for the compactor to top its thread after finishing already queued tasks
    private static final Integer STOP_SIGNAL = new Integer(-1);
    // signal for the compactor thread to stop its thread after finishing any active task but not taking more tasks;
    private static final Integer WAKE_SIGNAL = new Integer(-1);

    private final ReentrantLock startStopLock = new ReentrantLock();
    private volatile boolean stopInProgress = false;

    CompactionManager(HaloDBInternal dbInternal) {
        this.dbInternal = dbInternal;
        this.compactionRateLimiter = RateLimiter.create(dbInternal.options.getCompactionJobRate());
        this.compactionQueue = new LinkedBlockingQueue<>();
    }

    // If a file is being compacted we wait for it complete before stopping.
    boolean stopCompactionThread(boolean closeCurrentWriteFile, boolean awaitPending) throws IOException {
        stopInProgress = true;
        startStopLock.lock();
        try {
            if (isCompactionRunning()) {
                if (awaitPending) {
                    // we send a stop signal that will stop the thread after existing items in the queue complete
                    compactionQueue.put(STOP_SIGNAL);
                } else {
                    // set the running flag to false, then send the wake signal.  If the queue is empty it will immediately
                    // consume the signal to wake up the thread and stop.
                    // if the queue is not empty, then after the current task completes the 'isRunning' flag will stop it
                    isRunning = false;
                    compactionQueue.put(WAKE_SIGNAL);
                }
                compactionThread.join();
                if (closeCurrentWriteFile && currentWriteFile != null) {
                    currentWriteFile.flushToDisk();
                    currentWriteFile.getIndexFile().flushToDisk();
                    currentWriteFile.close();
                }
            }
        }
        catch (InterruptedException e) {
            logger.error("Error while waiting for compaction thread to stop", e);
            return false;
        }
        finally {
            stopInProgress = false;
            startStopLock.unlock();
        }
        return true;
    }

    void startCompactionThread() {
        startStopLock.lock();
        try {
            if (!isCompactionRunning()) {
                isRunning = true;
                compactionThread = new CompactionThread();
                compactionThread.start();
            }
        } finally {
            startStopLock.unlock();
        }
    }

    /**
     * Stop the compaction thread, blocking until it has stopped.
     * If awaitPending is true, stops after all outstanding compaction tasks in the queue
     * have completed.  Otherwise, stops after the current task completes.
     **/
    void pauseCompactionThread(boolean awaitPending) throws IOException {
        logger.info("Pausing compaction thread ...");
        stopCompactionThread(false, awaitPending);
    }

    void resumeCompaction() {
        logger.info("Resuming compaction thread");
        startCompactionThread();
    }

    int getCurrentWriteFileId() {
        return currentWriteFile != null ? currentWriteFile.getFileId() : -1;
    }

    boolean submitFileForCompaction(Integer fileId) {
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

    boolean isCompactionRunning() {
        return compactionThread != null && compactionThread.isAlive();
    }

    private class CompactionThread extends Thread {

        private long unFlushedData = 0;

        CompactionThread() {
            super("CompactionThread");

            setUncaughtExceptionHandler((t, e) -> {
                logger.error("Compaction thread crashed", e);
                if (currentWriteFile != null) {
                    try {
                        currentWriteFile.flushToDisk();
                    } catch (IOException ex) {
                        logger.error("Error while flushing " + currentWriteFile.getFileId() + " to disk", ex);
                    }
                    currentWriteFile = null;
                }
                currentWriteFileOffset = 0;

                if (!stopInProgress) {
                    startStopLock.lock();
                    try {
                        compactionThread = null;
                        if (isRunning) {
                          startCompactionThread();
                        }
                    } finally {
                        startStopLock.unlock();
                    }
                }
                else {
                    logger.info("Not restarting thread as the lock is held by stop compaction method.");
                }
            });
        }

        @Override
        public void run() {
            logger.info("Starting compaction thread ...");
            while (isRunning) {
                Integer fileToCompact = null;
                try {
                    fileToCompact = compactionQueue.poll(1, TimeUnit.SECONDS);
                    if (fileToCompact == STOP_SIGNAL) { // reference, not value equality on purpose, these are sentinel objects
                        logger.debug("Received a stop signal.");
                        // in this case, isRunning was not set to false already.  The signal had to work its way through the
                        // queue behind the other tasks.   So set 'isRunning' to false and break out of the loop to halt.
                        isRunning = false;
                        break;
                    }
                    if (fileToCompact == WAKE_SIGNAL || fileToCompact == null) {
                        // scenario:   the queue has a long list of files to compact.  We add this signal to the queue after
                        // setting 'isRunning' to false, so all we need to do is break out of the loop and it will shut down
                        // without processing more tasks.
                        // If we do break out of this loop with tasks in the queue, then this signal may still be in the queue
                        // behind those tasks.
                        // If the thread is resumed later, the signal will be processed after resuming the compactor.
                        // If we were to set 'isRunning' to false here, that would shut down the
                        // recently resumed thread when this signal arrived.
                        continue;
                    }
                    logger.debug("Compacting {} ...", fileToCompact);
                    copyFreshRecordsToNewFile(fileToCompact);
                    logger.debug("Completed compacting {} to {}", fileToCompact, getCurrentWriteFileId());
                    dbInternal.markFileAsCompacted(fileToCompact);
                    dbInternal.deleteHaloDBFile(fileToCompact);
                    fileToCompact = null;
                } catch (InterruptedException ie) {
                    break;
                } catch (Exception e) {
                    logger.error(String.format("Error while compacting file %d to %d", fileToCompact, getCurrentWriteFileId()), e);
                }
            }
            logger.info("Compaction thread stopped.");
        }

        // TODO: group and move adjacent fresh records together for performance.
        private void copyFreshRecordsToNewFile(int idOfFileToCompact) throws IOException {
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
            if (currentWriteFile == null || currentWriteFileOffset + recordSize > dbInternal.options
                .getMaxFileSize()) {
                forceRolloverCurrentWriteFile();
            }
        }
    }

    void forceRolloverCurrentWriteFile() throws IOException {
        if (currentWriteFile != null) {
            currentWriteFile.flushToDisk();
            currentWriteFile.getIndexFile().flushToDisk();
        }
        currentWriteFile = dbInternal.createHaloDBFile(HaloDBFile.FileType.COMPACTED_FILE);
        dbInternal.getDbDirectory().syncMetaData();
        currentWriteFileOffset = 0;
    }
}
