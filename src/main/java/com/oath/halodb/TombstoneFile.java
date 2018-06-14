/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Objects;

/**
 * @author Arjun Mannaly
 */
class TombstoneFile {
    private static final Logger logger = LoggerFactory.getLogger(TombstoneFile.class);

    private final File backingFile;
    private FileChannel channel;

    private final HaloDBOptions options;

    private long unFlushedData = 0;
    private long writeOffset = 0;

    static final String TOMBSTONE_FILE_NAME = ".tombstone";
    private static final String nullMessage = "Tombstone entry cannot be null";

    static TombstoneFile create(File dbDirectory, int fileId, HaloDBOptions options)  throws IOException {
        File file = getTombstoneFile(dbDirectory, fileId);

        while (!file.createNewFile()) {
            // file already exists try another one.
            fileId++;
            file = getTombstoneFile(dbDirectory, fileId);
        }

        TombstoneFile tombstoneFile = new TombstoneFile(file, options);
        tombstoneFile.open();

        return tombstoneFile;
    }

    TombstoneFile(File backingFile, HaloDBOptions options) {
        this.backingFile = backingFile;
        this.options = options;
    }

    void open() throws IOException {
        channel = new RandomAccessFile(backingFile, "rw").getChannel();
    }

    void close() throws IOException {
        if (channel != null) {
            channel.close();
        }
    }

    void delete() throws IOException {
        close();
        if (backingFile != null) {
            backingFile.delete();
        }
    }

    void write(TombstoneEntry entry) throws IOException {
        Objects.requireNonNull(entry, nullMessage);

        ByteBuffer[] contents = entry.serialize();
        long toWrite = 0;
        for (ByteBuffer buffer : contents) {
            toWrite += buffer.remaining();
        }
        long written = 0;
        while (written < toWrite) {
            written += channel.write(contents);
        }

        writeOffset += written;
        unFlushedData += written;
        if (options.getFlushDataSizeBytes() != -1 && unFlushedData > options.getFlushDataSizeBytes()) {
            channel.force(false);
            unFlushedData = 0;
        }
    }

    long getWriteOffset() {
        return writeOffset;
    }

    void flushToDisk() throws IOException {
        if (channel != null && channel.isOpen())
            channel.force(true);
    }

    /**
     * Copies to a new file those entries whose computed checksum matches the stored one.
     * Records in the file which occur after a corrupted record are discarded.
     * Current file is deleted after copy.
     * This method is called if we detect an unclean shutdown.
     */
    TombstoneFile repairFile(int newFileId) throws IOException {
        TombstoneFile newFile = create(backingFile.getParentFile(), newFileId, options);

        logger.info("Repairing tombstone file {}. Records with the correct checksum will be copied to {}", getName(), newFile.getName());

        TombstoneFileIterator iterator = newIteratorWithCheckForDataCorruption();
        int count = 0;
        while (iterator.hasNext()) {
            TombstoneEntry entry = iterator.next();
            if (entry == null) {
                logger.info("Found a corrupted entry in tombstone file {} after copying {} entries.", getName(), count);
                break;
            }
            count++;
            newFile.write(entry);
        }
        logger.info("Copied {} records from {} with size {} to {} with size {}. Deleting file ...", count, getName(), getSize(), newFile.getName(), newFile.getSize());
        newFile.flushToDisk();
        delete();
        return newFile;
    }

    String getName() {
        return backingFile.getName();
    }

    private long getSize() {
        return backingFile.length();
    }

    TombstoneFile.TombstoneFileIterator newIterator() throws IOException {
        return new TombstoneFile.TombstoneFileIterator(false);
    }

    // Returns null when it finds a corrupted entry.
    TombstoneFile.TombstoneFileIterator newIteratorWithCheckForDataCorruption() throws IOException {
        return new TombstoneFile.TombstoneFileIterator(true);
    }

    private static File getTombstoneFile(File dbDirectory, int fileId) {
        return Paths.get(dbDirectory.getPath(), fileId + TOMBSTONE_FILE_NAME).toFile();
    }

    class TombstoneFileIterator implements Iterator<TombstoneEntry> {

        private final ByteBuffer buffer;
        private final boolean discardCorruptedRecords;

        TombstoneFileIterator(boolean discardCorruptedRecords) throws IOException {
            buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
            this.discardCorruptedRecords = discardCorruptedRecords;
        }

        @Override
        public boolean hasNext() {
            return buffer.hasRemaining();
        }

        @Override
        public TombstoneEntry next() {
            if (hasNext()) {
                if (discardCorruptedRecords)
                    return TombstoneEntry.deserializeIfNotCorrupted(buffer);
                
                return TombstoneEntry.deserialize(buffer);
            }

            return null;
        }
    }
}
