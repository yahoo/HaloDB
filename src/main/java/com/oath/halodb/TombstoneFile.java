package com.oath.halodb;

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

    public void close() throws IOException {
        if (channel != null) {
            channel.close();
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
        if (options.flushDataSizeBytes != -1 && unFlushedData > options.flushDataSizeBytes) {
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

    TombstoneFile.TombstoneFileIterator newIterator() throws IOException {
        return new TombstoneFile.TombstoneFileIterator();
    }

    private static File getTombstoneFile(File dbDirectory, int fileId) {
        return Paths.get(dbDirectory.getPath(), fileId + TOMBSTONE_FILE_NAME).toFile();
    }

    class TombstoneFileIterator implements Iterator<TombstoneEntry> {

        private final ByteBuffer buffer;

        public TombstoneFileIterator() throws IOException {
            buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        }

        @Override
        public boolean hasNext() {
            return buffer.hasRemaining();
        }

        @Override
        public TombstoneEntry next() {
            if (hasNext()) {
                // TODO:
                // this can throw an error if the process crashed
                // and only part of the tombstone entry was written.
                return TombstoneEntry.deserialize(buffer);
            }

            return null;
        }
    }
}
