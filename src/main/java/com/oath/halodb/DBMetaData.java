/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.util.zip.CRC32;

import static java.nio.file.StandardOpenOption.*;
import static java.nio.file.StandardCopyOption.*;

/**
 * Represents the Metadata for the DB, stored in METADATA_FILE_NAME,
 * and contains methods to operate on it.
 */
class DBMetaData {

    /**
     * checksum         - 4 bytes
     * version          - 1 byte.
     * open             - 1 byte
     * sequence number  - 8 bytes.
     * io error         - 1 byte.
     * file size        - 4 byte.
     */
    private static final int META_DATA_SIZE = 4+1+1+8+1+4;
    private static final int CHECK_SUM_SIZE = 4;
    private static final int CHECK_SUM_OFFSET = 0;

    private long checkSum = 0;
    private int version = 0;
    private boolean open = false;
    private long sequenceNumber = 0;
    private boolean ioError = false;
    private int maxFileSize = 0;

    private final DBDirectory dbDirectory;

    static final String METADATA_FILE_NAME = "META";

    private static final Object lock = new Object();

    DBMetaData(DBDirectory dbDirectory) {
        this.dbDirectory = dbDirectory;
    }

    void loadFromFileIfExists() throws IOException {
        synchronized (lock) {
            Path metaFile = dbDirectory.getPath().resolve(METADATA_FILE_NAME);
            if (Files.exists(metaFile)) {
                try (SeekableByteChannel channel = Files.newByteChannel(metaFile)) {
                    ByteBuffer buff = ByteBuffer.allocate(META_DATA_SIZE);
                    channel.read(buff);
                    buff.flip();
                    checkSum = Utils.toUnsignedIntFromInt(buff.getInt());
                    version = Utils.toUnsignedByte(buff.get());
                    open = buff.get() != 0;
                    sequenceNumber = buff.getLong();
                    ioError = buff.get() != 0;
                    maxFileSize = buff.getInt();
                }
            }
        }
    }

    void storeToFile() throws IOException {
        synchronized (lock) {
            String tempFileName = METADATA_FILE_NAME + ".temp";
            Path tempFile = dbDirectory.getPath().resolve(tempFileName);
            Files.deleteIfExists(tempFile);
            try(FileChannel channel = FileChannel.open(tempFile, WRITE, CREATE, SYNC)) {
                ByteBuffer buff = ByteBuffer.allocate(META_DATA_SIZE);
                buff.position(CHECK_SUM_SIZE);
                buff.put((byte)version);
                buff.put((byte)(open ? 0xFF : 0));
                buff.putLong(sequenceNumber);
                buff.put((byte)(ioError ? 0xFF : 0));
                buff.putInt(maxFileSize);

                long crc32 = computeCheckSum(buff.array());
                buff.putInt(CHECK_SUM_OFFSET, (int)crc32);

                buff.flip();
                channel.write(buff);
                Files.move(tempFile, dbDirectory.getPath().resolve(METADATA_FILE_NAME), REPLACE_EXISTING, ATOMIC_MOVE);
                dbDirectory.syncMetaData();
            }
        }
    }

    private long computeCheckSum(byte[] header) {
        CRC32 crc32 = new CRC32();
        crc32.update(header, CHECK_SUM_OFFSET + CHECK_SUM_SIZE, META_DATA_SIZE - CHECK_SUM_SIZE);
        return crc32.getValue();
    }

    boolean isValid() {
        ByteBuffer buff = ByteBuffer.allocate(META_DATA_SIZE);
        buff.position(CHECK_SUM_SIZE);
        buff.put((byte)version);
        buff.put((byte)(open ? 0xFF : 0));
        buff.putLong(sequenceNumber);
        buff.put((byte)(ioError ? 0xFF : 0));
        buff.putInt(maxFileSize);

        return computeCheckSum(buff.array()) == checkSum;
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

    boolean isIOError() {
        return ioError;
    }

    void setIOError(boolean ioError) {
        this.ioError = ioError;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getMaxFileSize() {
        return maxFileSize;
    }

    public void setMaxFileSize(int maxFileSize) {
        this.maxFileSize = maxFileSize;
    }
}
