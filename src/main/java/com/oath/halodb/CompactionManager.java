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

    CompactionManager(HaloDBInternal dbInternal) {
        this.dbInternal = dbInternal;
        this.compactionRateLimiter = RateLimiter.create(dbInternal.options.compactionJobRate);
        this.compactionQueue = new LinkedBlockingQueue<>();
    }

    boolean stopCompactionThread() throws IOException {
        isRunning = false;
        if (compactionThread != null) {
            try {
                // We don't want to call interrupt on compaction thread as it
                // may interrupt IO operations and leave files in an inconsistent state.
                // instead we use -10101 as a stop signal.
                compactionQueue.put(-10101);
                compactionThread.join();
                if (currentWriteFile != null) {
                    currentWriteFile.flushToDisk();
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

    void resetStats() {
        numberOfRecordsCopied = numberOfRecordsReplaced
            = numberOfRecordsScanned = sizeOfRecordsCopied = sizeOfFilesDeleted = 0;
    }

    private class CompactionThread extends Thread {

        private long unFlushedData = 0;

        CompactionThread() {
            super("CompactionThread");

            setUncaughtExceptionHandler((t, e) -> {
                if (e instanceof RuntimeException && e.getCause() instanceof IOException) {
                    logger.error("IOException in Compaction thread. This is probably non-recoverable. Hence shutting down compaction");
                    isRunning = false;
                    try {
                        dbInternal.setIOErrorFlag();
                    } catch (IOException e1) {
                        logger.error("Error while setting IOError flag", e1);
                    }
                    return;
                }

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

            while (isRunning && !dbInternal.options.isCompactionDisabled) {
                try {
                    fileToCompact = compactionQueue.take();
                    if (fileToCompact == -10101) {
                        logger.debug("Received a stop signal.");
                        break;
                    }
                    logger.debug("Compacting {} ...", fileToCompact);
                    copyFreshRecordsToMergedFile(fileToCompact);
                    logger.debug("Completed compacting {} to {}", fileToCompact, getCurrentWriteFileId());
                    dbInternal.markFileAsCompacted(fileToCompact);
                    dbInternal.deleteHaloDBFile(fileToCompact);
                }
                catch (InterruptedException e) {
                    logger.error("Compaction thread interrupted", e);
                }
                catch (IOException e) {
                    logger.error("IO error while compacting file {} to {}", fileToCompact, getCurrentWriteFileId());
                    // IO errors are usually non-recoverable; problem with disk, lack of space etc.
                    throw new RuntimeException(e);
                }
                catch (Exception e){
                    logger.error("Error while compacting " + fileToCompact, e);
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

                RecordMetaDataForCache currentRecordMetaData = dbInternal.getKeyCache().get(key);

                if (isRecordFresh(indexFileEntry, currentRecordMetaData, idOfFileToCompact)) {
                    recordsCopied++;
                    compactionRateLimiter.acquire(recordSize);
                    rollOverCurrentWriteFile(recordSize);
                    sizeOfRecordsCopied += recordSize;

                    // fresh record, copy to merged file.
                    long transferred = readFrom.transferTo(recordOffset, recordSize, currentWriteFile.getChannel());

                    //TODO: for testing. remove.
                    if (transferred != recordSize) {
                        logger.error("Had to transfer {} but only did {}", recordSize, transferred);
                    }

                    unFlushedData += transferred;
                    if (dbInternal.options.flushDataSizeBytes != -1 && unFlushedData > dbInternal.options.flushDataSizeBytes) {
                        currentWriteFile.getChannel().force(false);
                        unFlushedData = 0;
                    }

                    IndexFileEntry newEntry = new IndexFileEntry(key, recordSize, currentWriteFileOffset, indexFileEntry.getSequenceNumber(), indexFileEntry.getFlags());
                    currentWriteFile.getIndexFile().write(newEntry);

                    int valueOffset = Utils.getValueOffset(currentWriteFileOffset, key);
                    RecordMetaDataForCache newMetaData = new RecordMetaDataForCache(currentWriteFile.getFileId(), valueOffset, currentRecordMetaData.getValueSize(), indexFileEntry.getSequenceNumber());

                    boolean updated = dbInternal.getKeyCache().replace(key, currentRecordMetaData, newMetaData);
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

        private boolean isRecordFresh(IndexFileEntry entry, RecordMetaDataForCache metaData, int idOfFileToMerge) {
            return metaData != null
                   && metaData.getFileId() == idOfFileToMerge
                   && metaData.getValueOffset() == Utils.getValueOffset(entry.getRecordOffset(), entry.getKey());
        }

        private void rollOverCurrentWriteFile(int recordSize) throws IOException {
            if (currentWriteFile == null ||  currentWriteFileOffset + recordSize > dbInternal.options.maxFileSize) {
                if (currentWriteFile != null) {
                    currentWriteFile.flushToDisk();
                    currentWriteFile.getIndexFile().flushToDisk();
                }
                currentWriteFile = dbInternal.createHaloDBFile(HaloDBFile.FileType.COMPACTED_FILE);
                currentWriteFileOffset = 0;
            }
        }
    }


    // Used only for tests. 
    boolean isMergeComplete() {
        if (compactionQueue.isEmpty()) {
            try {
                stopCompactionThread();
            } catch (IOException e) {
                logger.error("Error in isMergeComplete", e);
            }

            return true;
        }

        for (int fileId : compactionQueue) {
            // current write file and current compaction file will not be compacted.
            // if there are any other pending files return false.
            if (fileId != dbInternal.getCurrentWriteFileId() && fileId != getCurrentWriteFileId()) {
                return false;
            }
        }

        return true;
    }
}
