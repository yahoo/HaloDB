package amannaly;

import com.google.common.util.concurrent.RateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

class MergeJob {

    private static final Logger logger = LoggerFactory.getLogger(MergeJob.class);

    private final Set<Integer> fileIdsToMerge;
    private final HaloDBFile mergedFile;
    private final HaloDBInternal db;

    private long mergedFileOffset = 0;

    private long unFlushedData = 0;

    RateLimiter rateLimiter = RateLimiter.create(35 * 1024 * 1024);

    public MergeJob(Set<Integer> fileIdsToMerge, HaloDBFile mergedFile, HaloDBInternal db) {
        this.fileIdsToMerge = fileIdsToMerge;
        this.mergedFile = mergedFile;
        this.db = db;
    }

    //TODO: need a way to stop currently running job when db is closed.
    public void merge() {

        long start = System.currentTimeMillis();
        logger.info("About to start a merge run. Merging {} to {}", fileIdsToMerge, mergedFile.fileId);

        for (int fileId : fileIdsToMerge) {
            try {
                copyFreshRecordsToMergedFileUsingHintFile(fileId);
            } catch (Exception e) {
                logger.error("Error while compacting " + fileId, e);
            }
        }

        try {
            mergedFile.closeForWriting();
        } catch (IOException e) {
            logger.error("Error while closing merged file " + mergedFile.fileId, e);
        }

        long time = (System.currentTimeMillis()-start)/1000;
        logger.info("Completed merge run in {} seconds for file {}", time, mergedFile.fileId);
    }

    private static SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
    public static String printDate() {
        return sdf.format(new Date()) + ": ";
    }

    // TODO: group and move adjacent fresh records together for performance.
    private void copyFreshRecordsToMergedFileUsingHintFile(int idOfFileToMerge) throws IOException {

        //System.out.printf("Merging %s\n", idOfFileToMerge);

        //TODO: can I reuse read channel in the HaloDBFile, do I need to open another read channel
        //TODO: which performs better?
        //TODO: during transfer may be we can club together contiguous records?
        FileChannel readFrom =  db.getHaloDBFile(idOfFileToMerge).getReadChannel();

        HintFile.HintFileIterator iterator = db.getHaloDBFile(idOfFileToMerge).getHintFile().newIterator();

        while (iterator.hasNext()) {
            HintFileEntry hintFileEntry = iterator.next();
            byte[] key = hintFileEntry.getKey();
            long recordOffset = hintFileEntry.getRecordOffset();
            int recordSize = hintFileEntry.getRecordSize();

            RecordMetaDataForCache currentRecordMetaData = db.getKeyCache().get(key);

            if (currentRecordMetaData != null && currentRecordMetaData.fileId == idOfFileToMerge && currentRecordMetaData.offset == recordOffset) {
                rateLimiter.acquire(recordSize);

                // fresh record copy to merged file.
                long transferred = readFrom.transferTo(recordOffset, recordSize, mergedFile.getWriteChannel());
                assert transferred == recordSize;

                //TODO: for testing. remove.
                if (transferred != recordSize) {
                    logger.error("Had to transfer {} but only did {}", recordSize, transferred);
                }

                unFlushedData += transferred;

                if (db.options.flushDataSizeBytes != -1 && unFlushedData > db.options.flushDataSizeBytes) {
                    mergedFile.getWriteChannel().force(false);
                    unFlushedData = 0;
                }

                HintFileEntry newEntry = new HintFileEntry(key, recordSize, mergedFileOffset, hintFileEntry.getFlags());
                mergedFile.getHintFile().write(newEntry);

                RecordMetaDataForCache newMetaData = new RecordMetaDataForCache(mergedFile.fileId, mergedFileOffset,
                                                                recordSize);

                //TODO: if stale record, add the stale file map to remove later.
                db.getKeyCache().replace(key, currentRecordMetaData, newMetaData);
                mergedFileOffset += recordSize;
            }
        }
        db.deleteHaloDBFile(idOfFileToMerge);
    }

    private void copyFreshRecordsToMergedFile(int idOfFileToMerge) throws IOException {
        //System.out.printf("Merging %s\n", idOfFileToMerge);

        //TODO: can I reuse read channel in the BitcaskFile, do I need to open another read channel
        //TODO: which performs better.
        FileChannel readFrom =  db.getHaloDBFile(idOfFileToMerge).getReadChannel();

        long fileToMergeSize = readFrom.size();
        long fileToMergeOffset = 0;

        ByteBuffer header = ByteBuffer.allocate(Record.HEADER_SIZE);

        while (fileToMergeOffset < fileToMergeSize) {
            long temp = fileToMergeOffset;

            // read header from file.
            header.clear();
            int readSize = readFrom.read(header, temp);
            assert readSize == Record.HEADER_SIZE;
            temp += readSize;

            // read key size and value size from header.
            int keySize = header.getShort(Record.KEY_SIZE_OFFSET);
            int valueSize = header.getInt(Record.VALUE_SIZE_OFFSET);
            byte flag = header.get(Record.FLAGS_OFFSET);
            int recordSize = Record.HEADER_SIZE + keySize + valueSize;

            // read key from file.
            ByteBuffer keyBuff = ByteBuffer.allocate(keySize);
            readSize = readFrom.read(keyBuff, temp);
            assert readSize == keySize;

            keyBuff.flip();
            byte[] key = keyBuff.array();

            RecordMetaDataForCache currentRecordMetaData = db.getKeyCache().get(key);

            if (currentRecordMetaData != null && currentRecordMetaData.fileId == idOfFileToMerge && currentRecordMetaData.offset == fileToMergeOffset) {
                //System.out.printf("Key -> %d, current file %d\n", BitCaskDB.bytesToLong(key.toByteArray()), currentRecordMetaData.fileId);

                // fresh record copy to merged file.
                readFrom.transferTo(fileToMergeOffset, recordSize, mergedFile.getWriteChannel());
                HintFileEntry hintFileEntry = new HintFileEntry(key, recordSize, mergedFileOffset, flag);
                mergedFile.getHintFile().write(hintFileEntry);

                RecordMetaDataForCache
                    newMetaData = new RecordMetaDataForCache(mergedFile.fileId, mergedFileOffset, recordSize);

                db.getKeyCache().replace(key, currentRecordMetaData, newMetaData);
                mergedFileOffset += recordSize;
            }

            fileToMergeOffset += recordSize;
        }

        db.deleteHaloDBFile(idOfFileToMerge);
    }
}
