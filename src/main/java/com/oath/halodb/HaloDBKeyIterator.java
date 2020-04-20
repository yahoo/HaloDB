package com.oath.halodb;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HaloDBKeyIterator implements  Iterator<RecordKey>{
    private static final Logger logger = LoggerFactory.getLogger(HaloDBIterator.class);

    private Iterator<Integer> outer;
    private Iterator<IndexFileEntry> inner;
    private HaloDBFile currentFile;

    private RecordKey next;

    private final HaloDBInternal dbInternal;

    HaloDBKeyIterator(HaloDBInternal dbInternal) {
        this.dbInternal = dbInternal;
        outer = dbInternal.listDataFileIds().iterator();
    }

    @Override
    public boolean hasNext() {
        if (next != null) {
            return true;
        }

        try {
            // inner == null means this is the first time hasNext() is called.
            // use moveToNextFile() to move to the first file.
            if (inner == null && !moveToNextFile()) {
                return false;
            }

            do {
                if (readNextRecord()) {
                    return true;
                }
            } while (moveToNextFile());

            return false;

        } catch (IOException e) {
            logger.error("Error in Iterator", e);
            return false;
        }
    }

    @Override
    public RecordKey next() {
        if (hasNext()) {
            RecordKey key = next;
            next = null;
            return key;
        }
        throw new NoSuchElementException();
    }

    private boolean moveToNextFile() throws IOException {
        while (outer.hasNext()) {
            int fileId = outer.next();
            currentFile = dbInternal.getHaloDBFile(fileId);
            if (currentFile != null) {
                try {
                    inner = currentFile.getIndexFile().newIterator();
                    return true;
                } catch (ClosedChannelException e) {
                    if (dbInternal.isClosing()) {
                        //TODO: define custom Exception classes for HaloDB.
                        throw new RuntimeException("DB is closing");
                    }
                    logger.debug("Index file {} closed, probably by compaction thread. Skipping to next one", fileId);
                }
            }
            logger.debug("Data file {} deleted, probably by compaction thread. Skipping to next one", fileId);
        }

        return false;
    }

    private boolean readNextRecord() {
        while (inner.hasNext()) {
            IndexFileEntry entry = inner.next();
            try {
                try {
                    next = readValidRecordKey(entry);
                    if (next != null) {
                        return true;
                    }
                } catch (ClosedChannelException e) {
                    if (dbInternal.isClosing()) {
                        throw new RuntimeException("DB is closing");
                    }
                    logger.debug("Data file {} closed, probably by compaction thread. Skipping to next one", currentFile.getFileId());
                    break;
                }
            } catch (IOException e) {
                logger.info("Error in iterator", e);
                break;
            }
        }
        return false;
    }

    private RecordKey readValidRecordKey(IndexFileEntry entry) throws IOException {
        InMemoryIndexMetaData meta = Utils.getMetaData(entry, currentFile.getFileId());
        RecordKey key = null;
        if (dbInternal.isRecordFresh(entry.getKey(), meta)) {
            key = new RecordKey(entry.getKey());
        }
        return key;
    }
}
