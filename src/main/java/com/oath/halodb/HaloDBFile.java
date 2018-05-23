package com.oath.halodb;

import com.google.common.primitives.Ints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.regex.Matcher;

/**
 * Represents a data file and its associated index file.
 *
 * @author Arjun Mannaly
 */
class HaloDBFile {
    private static final Logger logger = LoggerFactory.getLogger(HaloDBFile.class);

    private FileChannel channel;

    private volatile int writeOffset;


    private final File backingFile;
    private IndexFile indexFile;
    private final int fileId;

    private final HaloDBOptions options;

    private long unFlushedData = 0;

    static final String DATA_FILE_NAME = ".data";
    static final String COMPACTED_DATA_FILE_NAME = ".datac";

    private final FileType fileType;

    private HaloDBFile(int fileId, File backingFile, IndexFile indexFile, FileType fileType,
                       FileChannel channel, HaloDBOptions options) throws IOException {
        this.fileId = fileId;
        this.backingFile = backingFile;
        this.indexFile = indexFile;
        this.fileType = fileType;
        this.channel = channel;
        this.writeOffset = Ints.checkedCast(channel.size());
        this.options = options;
    }

    byte[] readFromFile(int offset, int length) throws IOException {
        byte[] value = new byte[length];
        ByteBuffer valueBuf = ByteBuffer.wrap(value);
        int read = readFromFile(offset, valueBuf);
        assert read == length;

        return value;
    }

    int readFromFile(long position, ByteBuffer destinationBuffer) throws IOException {

        long currentPosition = position;
        int bytesRead;
        do {
            bytesRead = channel.read(destinationBuffer, currentPosition);
            currentPosition += bytesRead;
        } while (bytesRead != -1 && destinationBuffer.hasRemaining());

        return (int)(currentPosition - position);
    }

    private Record readRecord(int offset) throws HaloDBException, IOException {
        long tempOffset = offset;

        // read the header from disk.
        ByteBuffer headerBuf = ByteBuffer.allocate(Record.Header.HEADER_SIZE);
        int readSize = readFromFile(offset, headerBuf);
        if (readSize != Record.Header.HEADER_SIZE) {
            throw new HaloDBException("Corrupted header at " + offset + " in file " + fileId);
        }
        tempOffset += readSize;

        Record.Header header = Record.Header.deserialize(headerBuf);
        if (!Record.Header.verifyHeader(header)) {
            throw new HaloDBException("Corrupted header at " + offset + " in file " + fileId);
        }

        // read key-value from disk.
        ByteBuffer recordBuf = ByteBuffer.allocate(header.getKeySize() + header.getValueSize());
        readSize = readFromFile(tempOffset, recordBuf);
        if (readSize != recordBuf.capacity()) {
            throw new HaloDBException("Corrupted record at " + offset + " in file " + fileId);
        }

        Record record = Record.deserialize(recordBuf, header.getKeySize(), header.getValueSize());
        record.setHeader(header);
        int valueOffset = offset + Record.Header.HEADER_SIZE + header.getKeySize();
        record.setRecordMetaData(new RecordMetaDataForCache(fileId, valueOffset, header.getValueSize(), header.getSequenceNumber()));
        return record;
    }

    RecordMetaDataForCache writeRecord(Record record) throws IOException {
        writeToChannel(record.serialize(), channel);

        int recordSize = record.getRecordSize();
        int recordOffset = writeOffset;
        writeOffset += recordSize;

        IndexFileEntry indexFileEntry = new IndexFileEntry(record.getKey(), recordSize, recordOffset, record.getSequenceNumber(), record.getFlags());
        indexFile.write(indexFileEntry);

        int valueOffset = Utils.getValueOffset(recordOffset, record.getKey());
        return new RecordMetaDataForCache(fileId, valueOffset, record.getValue().length, record.getSequenceNumber());
    }

    void rebuildIndexFile() throws IOException {
        indexFile.delete();

        indexFile = new IndexFile(fileId, backingFile.getParentFile(), options);
        indexFile.create();

        HaloDBFileIterator iterator = new HaloDBFileIterator();
        int offset = 0;
        while (iterator.hasNext()) {
            Record record = iterator.next();
            IndexFileEntry indexFileEntry = new IndexFileEntry(record.getKey(), record.getRecordSize(), offset, record.getSequenceNumber(), record.getFlags());
            indexFile.write(indexFileEntry);
            offset += record.getRecordSize();
        }
    }

    /**
     * Copies to a new file those records whose computed checksum matches the stored one.
     * Records in the file which occur after a corrupted record are discarded.
     * Index file is also recreated.
     *
     * Current file is deleted after copy.
     *
     * This method is called if we detect an unclean shutdown.
     */
    HaloDBFile repairFile(int newFileId) throws IOException {
        HaloDBFile newFile = create(backingFile.getParentFile(), newFileId, options, fileType);

        logger.info("Repairing file {}. Records with the correct checksum will be copied to {}", fileId, newFile.fileId);

        HaloDBFileIterator iterator = new HaloDBFileIterator();
        int count = 0;
        while (iterator.hasNext()) {
            Record record = iterator.next();
            // if the header is corrupted iterator will return null.
            if (record != null && record.verifyChecksum()) {
                newFile.writeRecord(record);
                count++;
            }
            else {
                logger.info("Found a corrupted record after copying {} records", count);
                break;
            }
        }
        logger.info("Copied {} records from {} with size {} to {} with size {}. Deleting file ...", count, fileId, getSize(), newFile.fileId, newFile.getSize());
        newFile.flushToDisk();
        newFile.indexFile.flushToDisk();
        delete();
        return newFile;
    }

    private long writeToChannel(ByteBuffer[] buffers, FileChannel writeChannel) throws IOException {
        long toWrite = 0;
        for (ByteBuffer buffer : buffers) {
            toWrite += buffer.remaining();
        }

        long written = 0;
        while (written < toWrite) {
            written += writeChannel.write(buffers);
        }

        unFlushedData += written;

        if (options.flushDataSizeBytes != -1 && unFlushedData > options.flushDataSizeBytes) {
            //TODO: since metadata is not flushed file corruption can happen when process crashes.
            writeChannel.force(false);
            unFlushedData = 0;
        }
        return written;
    }

    void flushToDisk() throws IOException {
        if (channel != null && channel.isOpen())
            channel.force(true);
    }

    long getWriteOffset() {
        return writeOffset;
    }

    void setWriteOffset(int writeOffset) {
        this.writeOffset = writeOffset;
    }

    long getSize() {
        return writeOffset;
    }

    IndexFile getIndexFile() {
        return indexFile;
    }

    FileChannel getChannel() {
        return channel;
    }

    FileType getFileType() {
        return fileType;
    }

    int getFileId() {
        return fileId;
    }

    static HaloDBFile openForReading(File haloDBDirectory, File filename, FileType fileType, HaloDBOptions options) throws IOException {
        int fileId = HaloDBFile.getFileTimeStamp(filename);
        FileChannel channel = new RandomAccessFile(filename, "r").getChannel();
        IndexFile indexFile = new IndexFile(fileId, haloDBDirectory, options);
        indexFile.open();

        return new HaloDBFile(fileId, filename, indexFile, fileType, channel, options);
    }

    static HaloDBFile create(File haloDBDirectory, int fileId, HaloDBOptions options, FileType fileType) throws IOException {
        BiFunction<File, Integer, File> toFile = (fileType == FileType.DATA_FILE) ? HaloDBFile::getDataFile : HaloDBFile::getCompactedDataFile;

        File file = toFile.apply(haloDBDirectory, fileId);
        while (!file.createNewFile()) {
            // file already exists try another one.
            fileId++;
            file = toFile.apply(haloDBDirectory, fileId);
        }

        FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
        //TODO: setting the length might improve performance.
        //file.setLength(max_);

        IndexFile indexFile = new IndexFile(fileId, haloDBDirectory, options);
        indexFile.create();

        return new HaloDBFile(fileId, file, indexFile, fileType, channel, options);
    }

    HaloDBFileIterator newIterator() throws IOException {
        return new HaloDBFileIterator();
    }

    void close() throws IOException {
        if (channel != null) {
            channel.close();
        }
        if (indexFile != null) {
            indexFile.close();
        }
    }

    void delete() throws IOException {
        close();
        if (backingFile != null)
            backingFile.delete();

        if (indexFile != null)
            indexFile.delete();
    }

    private static File getDataFile(File haloDBDirectory, int fileId) {
        return Paths.get(haloDBDirectory.getPath(), fileId + DATA_FILE_NAME).toFile();
    }

    private static File getCompactedDataFile(File haloDBDirectory, int fileId) {
        return Paths.get(haloDBDirectory.getPath(), fileId + COMPACTED_DATA_FILE_NAME).toFile();
    }

    static FileType findFileType(File file) {
        String name = file.getName();
        return name.endsWith(COMPACTED_DATA_FILE_NAME) ? FileType.COMPACTED_FILE : FileType.DATA_FILE;
    }

    static int getFileTimeStamp(File file) {
        Matcher matcher = Constants.DATA_FILE_PATTERN.matcher(file.getName());
        matcher.find();
        String s = matcher.group(1);
        return Integer.parseInt(s);
    }

    /**
     * This iterator is intended only to be used internally as it behaves bit differently
     * from expected Iterator behavior: If a record is corrupted next() will return null although hasNext()
     * returns true.
     */
    class HaloDBFileIterator implements Iterator<Record> {

        private final int endOffset;
        private int currentOffset = 0;

        HaloDBFileIterator() throws IOException {
            this.endOffset = Ints.checkedCast(channel.size());
        }

        @Override
        public boolean hasNext() {
            return currentOffset < endOffset;
        }

        @Override
        public Record next() {
            Record record;
            try {
                record = readRecord(currentOffset);
            } catch (IOException | HaloDBException e) {
                // we have encountered an error, probably because record is corrupted.
                // we skip rest of the file and return null.
                logger.error("Error in iterator", e);
                currentOffset = endOffset;
                return null;
            }
            currentOffset += record.getRecordSize();
            return record;
        }
    }

    enum FileType {
        DATA_FILE, COMPACTED_FILE;
    }
}
