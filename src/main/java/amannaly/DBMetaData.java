package amannaly;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import static java.nio.file.StandardOpenOption.*;
import static java.nio.file.StandardCopyOption.*;

/**
 * Represents the Metadata for the DB, stored in METADATA_FILE_NAME,
 * and contains methods to operate on it.
 *
 * @author Arjun Mannaly
 */
class DBMetaData {

    /**
     * open             - 1 byte
     * sequence number  - 8 bytes.
     */
    final static int META_DATA_SIZE = 1+8;

    private boolean open = false;
    private long sequenceNumber = 0;

    private final String dbDirectory;

    static final String METADATA_FILE_NAME = "META";

    DBMetaData(String dbDirectory) {
        this.dbDirectory = dbDirectory;
    }

    synchronized void loadFromFile() throws IOException {
        Path metaFile = Paths.get(dbDirectory, METADATA_FILE_NAME);
        if (Files.exists(metaFile)) {
            try (SeekableByteChannel channel = Files.newByteChannel(metaFile)) {
                ByteBuffer buff = ByteBuffer.allocate(META_DATA_SIZE);
                channel.read(buff);
                buff.flip();
                open = buff.get() != 0;
                sequenceNumber = buff.getLong();
            }
        }
    }

    synchronized void storeToFile() throws IOException {
        String tempFileName = METADATA_FILE_NAME + ".temp";
        Path tempFile = Paths.get(dbDirectory, tempFileName);
        Files.deleteIfExists(tempFile);
        try(FileChannel channel = FileChannel.open(tempFile, WRITE, CREATE, SYNC)) {
            ByteBuffer buff = ByteBuffer.allocate(META_DATA_SIZE);
            buff.put((byte)(open ? 0xFF : 0));
            buff.putLong(sequenceNumber);
            buff.flip();
            channel.write(buff);
            Files.move(tempFile, Paths.get(dbDirectory, METADATA_FILE_NAME), REPLACE_EXISTING, ATOMIC_MOVE);
        }
    }

    boolean isOpen() {
        return open;
    }

    void setOpen(boolean open) {
        this.open = open;
    }

    long getSequenceNumber() {
        return sequenceNumber;
    }

    void setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }
}
