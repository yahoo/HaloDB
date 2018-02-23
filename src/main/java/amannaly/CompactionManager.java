package amannaly;

import com.google.common.util.concurrent.RateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Optional;

/**
 * @author Arjun Mannaly
 */
class CompactionManager extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(CompactionManager.class);

    private  final HaloDBInternal dbInternal;

    private volatile boolean isRunning = true;

    private final int intervalBetweenRunsInSeconds;

    private final RateLimiter compactionRateLimiter;

    private HaloDBFile currentWriteFile = null;
    private int currentWriteFileOffset = 0;
    private long unFlushedData = 0;

    //TODO; used scheduled thread pool executor.
    CompactionManager(HaloDBInternal dbInternal, int intervalBetweenRunsInSeconds) {
        super("CompactionManager");
        this.dbInternal = dbInternal;
        this.intervalBetweenRunsInSeconds = intervalBetweenRunsInSeconds;
        this.compactionRateLimiter = RateLimiter.create(dbInternal.options.compactionJobRate);

        this.setUncaughtExceptionHandler((t, e) -> {
            logger.error("Merge thread crashed", e);
            //TODO: error handling logic.
        });
    }

    @Override
    public void run() {
        while (isRunning && !dbInternal.options.isMergeDisabled) {
            long nextRun = System.currentTimeMillis() + intervalBetweenRunsInSeconds * 1000;

            int fileToCompact = dbInternal.getFileToCompact();
            if (fileToCompact != -1) {
                try {
                    logger.debug("Compacting {}.data ...", fileToCompact);
                    copyFreshRecordsToMergedFile(fileToCompact);
                    logger.debug("Completed compacting {}.data to {}.datac", fileToCompact, Optional.ofNullable(currentWriteFile).map(f -> f.fileId).orElse(-1));
                    dbInternal.markFileAsCompacted(fileToCompact);
                    // TODO: there is a chance of data loss if this file is deleted before the data that was moved from
                    // this file to the merged file hits the disk. To prevent that fsync the data before deleting the file.
                    dbInternal.deleteHaloDBFile(fileToCompact);
                } catch (Exception e) {
                    logger.error("Error while compacting " + fileToCompact, e);
                }
            }

            long msToSleep = Math.max(0, nextRun-System.currentTimeMillis());
            try {
                Thread.sleep(msToSleep);
            } catch (InterruptedException e) {
                logger.error("Compaction thread interrupted", e);
            }
        }
    }


    // TODO: group and move adjacent fresh records together for performance.
    private void copyFreshRecordsToMergedFile(int idOfFileToMerge) throws IOException {
        FileChannel readFrom =  dbInternal.getHaloDBFile(idOfFileToMerge).getChannel();

        IndexFile.IndexFileIterator iterator = dbInternal.getHaloDBFile(idOfFileToMerge).getIndexFile().newIterator();

        while (iterator.hasNext()) {
            IndexFileEntry indexFileEntry = iterator.next();
            byte[] key = indexFileEntry.getKey();
            long recordOffset = indexFileEntry.getRecordOffset();
            int recordSize = indexFileEntry.getRecordSize();

            RecordMetaDataForCache currentRecordMetaData = dbInternal.getKeyCache().get(key);

            if (isRecordFresh(indexFileEntry, currentRecordMetaData, idOfFileToMerge)) {
                compactionRateLimiter.acquire(recordSize);
                rollOverCurrentWriteFile(recordSize);

                // fresh record, copy to merged file.
                long transferred = readFrom.transferTo(recordOffset, recordSize, currentWriteFile.getChannel());
                assert transferred == recordSize;

                //TODO: for testing. remove.
                if (transferred != recordSize) {
                    logger.error("Had to transfer {} but only did {}", recordSize, transferred);
                }

                unFlushedData += transferred;
                if (dbInternal.options.flushDataSizeBytes != -1 && unFlushedData > dbInternal.options.flushDataSizeBytes) {
                    //TODO: since metadata is not flushed file corruption can happen when process crashes.
                    currentWriteFile.getChannel().force(false);
                    unFlushedData = 0;
                }

                IndexFileEntry newEntry = new IndexFileEntry(key, recordSize, currentWriteFileOffset, indexFileEntry.getSequenceNumber(), indexFileEntry.getFlags());
                currentWriteFile.getIndexFile().write(newEntry);

                int valueOffset = Utils.getValueOffset(currentWriteFileOffset, key);
                RecordMetaDataForCache newMetaData = new RecordMetaDataForCache(currentWriteFile.fileId, valueOffset, currentRecordMetaData.getValueSize(), indexFileEntry.getSequenceNumber());

                boolean updated = dbInternal.getKeyCache().replace(key, currentRecordMetaData, newMetaData);
                if (!updated) {
                    // write thread wrote a new version while this version was being compacted.
                    // therefore, this version is stale.
                    dbInternal.updateStaleDataMap(currentWriteFile.fileId, recordSize);
                }
                currentWriteFileOffset += recordSize;
            }
        }
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
            }
            currentWriteFile = dbInternal.createHaloDBFile(HaloDBFile.FileType.COMPACTED_FILE);
            currentWriteFileOffset = 0;
        }
    }

    void stopThread() {
        isRunning = false;
    }

    int getCurrentWriteFileId() {
        if (currentWriteFile == null)
            return -1;

        return currentWriteFile.fileId;
    }
}
