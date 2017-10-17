package amannaly;

import com.google.protobuf.ByteString;

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
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            mergedFile.closeForWriting();
        } catch (IOException e) {
            e.printStackTrace();
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

        //TODO: can I reuse read channel in the BitcaskFile, do I need to open another read channel
        //TODO: which performs better.
        FileChannel readFrom =  db.getHaloDBFile(idOfFileToMerge).getReadChannel();

        HintFile.HintFileIterator iterator = db.getHaloDBFile(idOfFileToMerge).getHintFile().newIterator();

        while (iterator.hasNext()) {
            HintFileEntry hintFileEntry = iterator.next();
            ByteString key = hintFileEntry.getKey();
            long recordOffset = hintFileEntry.getRecordOffset();
            int recordSize = hintFileEntry.getRecordSize();

            RecordMetaData currentRecordMetaData = db.getKeyCache().get(key);

            if (currentRecordMetaData != null && currentRecordMetaData.fileId == idOfFileToMerge && currentRecordMetaData.offset == recordOffset) {
                // fresh record copy to merged file.
                readFrom.transferTo(recordOffset, recordSize, mergedFile.getWriteChannel());
                HintFileEntry newEntry = new HintFileEntry(key, recordSize, mergedFileOffset);
                mergedFile.getHintFile().write(newEntry);

                RecordMetaData newMetaData = new RecordMetaData(mergedFile.fileId, mergedFileOffset,
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
            int recordSize = Record.HEADER_SIZE + keySize + valueSize;

            // read key from file.
            //TODO: wrapping an array of bytes if more efficient.
            //TODO: as it avoids array copy.
            ByteBuffer keyBuff = ByteBuffer.allocate(keySize);
            readSize = readFrom.read(keyBuff, temp);
            assert readSize == keySize;

            keyBuff.flip();
            ByteString key = ByteString.copyFrom(keyBuff);

            RecordMetaData currentRecordMetaData = db.getKeyCache().get(key);

            if (currentRecordMetaData != null && currentRecordMetaData.fileId == idOfFileToMerge && currentRecordMetaData.offset == fileToMergeOffset) {
                //System.out.printf("Key -> %d, current file %d\n", BitCaskDB.bytesToLong(key.toByteArray()), currentRecordMetaData.fileId);

                // fresh record copy to merged file.
                readFrom.transferTo(fileToMergeOffset, recordSize, mergedFile.getWriteChannel());
                HintFileEntry hintFileEntry = new HintFileEntry(key, recordSize, mergedFileOffset);
                mergedFile.getHintFile().write(hintFileEntry);

                RecordMetaData newMetaData = new RecordMetaData(mergedFile.fileId, mergedFileOffset, recordSize);

                db.getKeyCache().replace(key, currentRecordMetaData, newMetaData);
                mergedFileOffset += recordSize;
            }

            fileToMergeOffset += recordSize;
        }

        db.deleteHaloDBFile(idOfFileToMerge);
    }
}
