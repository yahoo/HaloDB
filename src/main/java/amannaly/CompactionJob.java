package amannaly;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Set;

/**
 * @author Arjun Mannaly
 */
class CompactionJob {
    private static final Logger logger = LoggerFactory.getLogger(CompactionJob.class);

    private final Set<Integer> fileIdsToMerge;
    private final HaloDBFile mergedFile;
    private final HaloDBInternal db;

    // TODO: use 35 MB for tests.
    private final RateLimiter compactionRateLimiter;

    private int mergedFileOffset = 0;

    private long unFlushedData = 0;

    CompactionJob(Set<Integer> fileIdsToMerge, HaloDBFile mergedFile, HaloDBInternal db) {
        this.fileIdsToMerge = fileIdsToMerge;
        this.mergedFile = mergedFile;
        this.db = db;
        this.compactionRateLimiter = RateLimiter.create(db.options.compactionJobRate);
    }

    void run() {

        long start = System.currentTimeMillis();
        logger.info("About to start a merge run. Merging {} to {}", fileIdsToMerge, mergedFile.fileId);

        for (int fileId : fileIdsToMerge) {
            try {
                copyFreshRecordsToMergedFileUsingIndexFile(fileId);
            } catch (Exception e) {
                logger.error("Error while compacting " + fileId, e);
            }
        }

        long time = (System.currentTimeMillis()-start)/1000;
        logger.info("Completed merge run in {} seconds for file {}", time, mergedFile.fileId);
    }

    // TODO: group and move adjacent fresh records together for performance.
    private void copyFreshRecordsToMergedFileUsingIndexFile(int idOfFileToMerge) throws IOException {
        FileChannel readFrom =  db.getHaloDBFile(idOfFileToMerge).getChannel();

        IndexFile.IndexFileIterator iterator = db.getHaloDBFile(idOfFileToMerge).getIndexFile().newIterator();

        while (iterator.hasNext()) {
            IndexFileEntry indexFileEntry = iterator.next();
            byte[] key = indexFileEntry.getKey();
            long recordOffset = indexFileEntry.getRecordOffset();
            int recordSize = indexFileEntry.getRecordSize();

            RecordMetaDataForCache currentRecordMetaData = db.getKeyCache().get(key);

            if (isRecordFresh(indexFileEntry, currentRecordMetaData, idOfFileToMerge)) {
                compactionRateLimiter.acquire(recordSize);

                // fresh record copy to merged file.
                long transferred = readFrom.transferTo(recordOffset, recordSize, mergedFile.getChannel());
                assert transferred == recordSize;

                //TODO: for testing. remove.
                if (transferred != recordSize) {
                    logger.error("Had to transfer {} but only did {}", recordSize, transferred);
                }

                unFlushedData += transferred;

                if (db.options.flushDataSizeBytes != -1 && unFlushedData > db.options.flushDataSizeBytes) {
                    mergedFile.getChannel().force(false);
                    unFlushedData = 0;
                }

                IndexFileEntry newEntry = new IndexFileEntry(key, recordSize, mergedFileOffset, indexFileEntry.getSequenceNumber(), indexFileEntry.getFlags());
                mergedFile.getIndexFile().write(newEntry);

                RecordMetaDataForCache newMetaData = new RecordMetaDataForCache(mergedFile.fileId, mergedFileOffset, recordSize, indexFileEntry.getSequenceNumber());

                if (!indexFileEntry.isTombStone()) {
                    //TODO: if stale record, add the stale file map to remove later.
                    db.getKeyCache().replace(key, currentRecordMetaData, newMetaData);
                }

                mergedFileOffset += recordSize;
            }
        }
        db.deleteHaloDBFile(idOfFileToMerge);
    }

    private boolean isRecordFresh(IndexFileEntry entry, RecordMetaDataForCache metaData, int idOfFileToMerge) {
        return (metaData == null && entry.isTombStone()) ||
                (metaData != null && metaData.getFileId() == idOfFileToMerge && metaData.getOffset() == entry.getRecordOffset());
    }

    private void copyFreshRecordsToMergedFile(int idOfFileToMerge) throws IOException {
        FileChannel readFrom =  db.getHaloDBFile(idOfFileToMerge).getChannel();

        long fileToMergeSize = readFrom.size();
        long fileToMergeOffset = 0;

        ByteBuffer header = ByteBuffer.allocate(Record.Header.HEADER_SIZE);

        while (fileToMergeOffset < fileToMergeSize) {
            long temp = fileToMergeOffset;

            // read header from file.
            header.clear();
            int readSize = readFrom.read(header, temp);
            assert readSize == Record.Header.HEADER_SIZE;
            temp += readSize;

            // read key size and value size from header.
            int keySize = header.getShort(Record.Header.KEY_SIZE_OFFSET);
            int valueSize = header.getInt(Record.Header.VALUE_SIZE_OFFSET);
            byte flag = header.get(Record.Header.FLAGS_OFFSET);
            long sequenceNumber = header.get(Record.Header.SEQUENCE_NUMBER_OFFSET);
            int recordSize = Record.Header.HEADER_SIZE + keySize + valueSize;

            // read key from file.
            ByteBuffer keyBuff = ByteBuffer.allocate(keySize);
            readSize = readFrom.read(keyBuff, temp);
            assert readSize == keySize;

            keyBuff.flip();
            byte[] key = keyBuff.array();

            RecordMetaDataForCache currentRecordMetaData = db.getKeyCache().get(key);

            if (currentRecordMetaData != null && currentRecordMetaData.getFileId() == idOfFileToMerge && currentRecordMetaData.getOffset() == fileToMergeOffset) {
                //System.out.printf("Key -> %d, current file %d\n", BitCaskDB.bytesToLong(key.toByteArray()), currentRecordMetaData.fileId);

                // fresh record copy to merged file.
                readFrom.transferTo(fileToMergeOffset, recordSize, mergedFile.getChannel());
                IndexFileEntry indexFileEntry = new IndexFileEntry(key, recordSize, mergedFileOffset, sequenceNumber, flag);
                mergedFile.getIndexFile().write(indexFileEntry);

                RecordMetaDataForCache
                    newMetaData = new RecordMetaDataForCache(mergedFile.fileId, mergedFileOffset, recordSize, indexFileEntry.getSequenceNumber());

                db.getKeyCache().replace(key, currentRecordMetaData, newMetaData);
                mergedFileOffset += recordSize;
            }

            fileToMergeOffset += recordSize;
        }

        db.deleteHaloDBFile(idOfFileToMerge);
    }
}
