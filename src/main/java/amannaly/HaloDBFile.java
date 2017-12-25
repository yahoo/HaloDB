package amannaly;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.regex.Matcher;

import static amannaly.Record.Header.HEADER_SIZE;

/**
 * @author Arjun Mannaly
 */
class HaloDBFile {

	private FileChannel channel;

	private long writeOffset;

	long getWriteOffset() {
		return writeOffset;
	}

	private final File backingFile;
	private IndexFile indexFile;
	final int fileId;

	private final HaloDBOptions options;

	private long unFlushedData = 0;

    static final String DATA_FILE_NAME = ".data";

    private HaloDBFile(int fileId, File backingFile, IndexFile indexFile,
                       FileChannel channel, HaloDBOptions options) throws IOException {
		this.fileId = fileId;
		this.backingFile = backingFile;
		this.indexFile = indexFile;
		this.channel = channel;
		this.writeOffset = channel.size();
		this.options = options;
	}
	
	Record read(long offset, int length) throws IOException {
		Record record = readRecord(offset);
		assert length == record.getRecordSize();

		return record;
	}

	private int readFromFile(long position, ByteBuffer destinationBuffer) throws IOException {

		long currentPosition = position;
		int bytesRead;
		do {
			bytesRead = channel.read(destinationBuffer, currentPosition);
			currentPosition += bytesRead;
		} while (bytesRead != -1 && destinationBuffer.hasRemaining());

		return (int)(currentPosition - position);
	}

	Record readRecord(long offset) throws IOException {
		long tempOffset = offset;

		// read the header from disk.
		ByteBuffer headerBuf = ByteBuffer.allocate(HEADER_SIZE);
		int readSize = readFromFile(offset, headerBuf);
		assert readSize == HEADER_SIZE;
		tempOffset += readSize;

		Record.Header header = Record.Header.deserialize(headerBuf);

		// read key-value from disk.
		ByteBuffer recordBuf = ByteBuffer.allocate(header.getRecordSize());
		readFromFile(tempOffset, recordBuf);

		Record record = Record.deserialize(recordBuf, header.getKeySize(), header.getValueSize());
		record.setRecordMetaData(new RecordMetaDataForCache(fileId, offset, header.getRecordSize(), header.getSequenceNumber()));
		return record;
	}

	RecordMetaDataForCache writeRecord(Record record) throws IOException {

		long start = System.nanoTime();
		writeToChannel(record.serialize(), channel);

		int recordSize = record.getRecordSize();
		long recordOffset = writeOffset;
		writeOffset += recordSize;

		IndexFileEntry indexFileEntry = new IndexFileEntry(record.getKey(), recordSize, recordOffset, record.getSequenceNumber(), record.getFlags());
		indexFile.write(indexFileEntry);

		HaloDB.recordWriteLatency(System.nanoTime() - start);

		return new RecordMetaDataForCache(fileId, recordOffset, recordSize, record.getSequenceNumber());
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
			writeChannel.force(false);
			unFlushedData = 0;
		}

		return written;
	}

	public long getSize() {
		return backingFile.length();
	}

	IndexFile getIndexFile() {
		return indexFile;
	}

	FileChannel getChannel() {
		return channel;
	}

	static HaloDBFile openForReading(File haloDBDirectory, File filename, HaloDBOptions options) throws IOException {

		int fileId = HaloDBFile.getFileTimeStamp(filename);
		
		FileChannel rch = new RandomAccessFile(filename, "r").getChannel();

		IndexFile indexFile = new IndexFile(fileId, haloDBDirectory, options);
		indexFile.open();

		return new HaloDBFile(fileId, filename, indexFile, rch, options);
	}

	static HaloDBFile create(File haloDBDirectory, int fileId, HaloDBOptions options) throws IOException {
		File file = getDataFile(haloDBDirectory, fileId);
		while (!file.createNewFile()) {
            // file already exists try another one.
		    fileId++;
            file = getDataFile(haloDBDirectory, fileId);
        }

		FileChannel channel = new RandomAccessFile(file, "rw").getChannel();

		//TODO: setting the length might improve performance.
		//file.setLength(max_);

		IndexFile indexFile = new IndexFile(fileId, haloDBDirectory, options);
		indexFile.open();

		return new HaloDBFile(fileId, file, indexFile, channel, options);
	}

	HaloDBFileIterator newIterator() throws IOException {
		return new HaloDBFileIterator();
	}

    void close() throws IOException {
		if (channel != null) {
			channel.close();
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

	static int getFileTimeStamp(File file) {
		Matcher matcher = Constants.DATA_FILE_PATTERN.matcher(file.getName());
		matcher.find();
		String s = matcher.group(1);
		return Integer.parseInt(s);
	}


	//TODO: we need to return only fresh files.
	//TODO: scan Index file iterator for performance.
	public class HaloDBFileIterator implements Iterator<Record> {

		private final long endOffset;
		private long currentOffset = 0;

		HaloDBFileIterator() throws IOException {
			this.endOffset = channel.size();
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
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
			currentOffset += record.getRecordSize();
			return record;
		}
	}
}
