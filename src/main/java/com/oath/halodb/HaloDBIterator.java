/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Arjun Mannaly
 */
public class HaloDBIterator implements Iterator<Record> {
    private static final Logger logger = LoggerFactory.getLogger(HaloDBIterator.class);

    private Iterator<Integer> outer;
    private Iterator<IndexFileEntry> inner;
    private HaloDBFile currentFile;

    private Record next;

    private final HaloDBInternal dbInternal;

    HaloDBIterator(HaloDBInternal dbInternal) {
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
    public Record next() {
        if (hasNext()) {
            Record record = next;
            next = null;
            return record;
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
                    next = readRecordFromDataFile(entry);
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

    private Record readRecordFromDataFile(IndexFileEntry entry) throws IOException {
        InMemoryIndexMetaData meta = Utils.getMetaData(entry, currentFile.getFileId());
        Record record = null;
        if (dbInternal.isRecordFresh(entry.getKey(), meta)) {
            byte[] value = currentFile.readFromFile(
                Utils.getValueOffset(entry.getRecordOffset(), entry.getKey()),
                Utils.getValueSize(entry.getRecordSize(), entry.getKey()));
            record = new Record(entry.getKey(), value);
            record.setRecordMetaData(meta);
        }
        return record;
    }
}
