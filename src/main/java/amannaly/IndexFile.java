package amannaly;

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
class IndexFile {

    private final int fileId;
    private final File dbDirectory;

    private FileChannel channel;

    private final HaloDBOptions options;

    private long unFlushedData = 0;

    static final String INDEX_FILE_NAME = ".index";
    private static final String nullMessage = "Index file entry cannot be null";

    IndexFile(int fileId, File dbDirectory, HaloDBOptions options) {
        this.fileId = fileId;
        this.dbDirectory = dbDirectory;
        this.options = options;
    }

    public void open() throws IOException {
        File file = getIndexFile();
        file.createNewFile();

        channel = new RandomAccessFile(file, "rw").getChannel();
    }

    public void close() throws IOException {
        if (channel != null) {
            channel.close();
        }
    }

    void delete() throws IOException {
        if (channel != null && channel.isOpen())
            channel.close();

        getIndexFile().delete();
    }

    void write(IndexFileEntry entry) throws IOException {
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

        unFlushedData += written;
        if (options.flushDataSizeBytes != -1 && unFlushedData > options.flushDataSizeBytes) {
            channel.force(false);
            unFlushedData = 0;
        }
    }

    void flushToDisk() throws IOException {
        if (channel != null && channel.isOpen())
            channel.force(true);
    }

    IndexFileIterator newIterator() throws IOException {
        return new IndexFileIterator();
    }

    private File getIndexFile() {
        return Paths.get(dbDirectory.getPath(), fileId + INDEX_FILE_NAME).toFile();
    }

    public class IndexFileIterator implements Iterator<IndexFileEntry> {

        private final ByteBuffer buffer;

        //TODO: index files are not that large, need to check the
        // performance since we are memory mapping it.
        public IndexFileIterator() throws IOException {
            buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        }

        @Override
        public boolean hasNext() {
            return buffer.hasRemaining();
        }

        @Override
        public IndexFileEntry next() {
            if (hasNext()) {
                return IndexFileEntry.deserialize(buffer);
            }
            return null;
        }
    }
}
