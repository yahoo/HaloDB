package amannaly;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.regex.Matcher;

import static amannaly.Record.HEADER_SIZE;

public class HaloDBFile {

	private FileChannel writeChannel;
	private FileChannel readChannel;

	private long writeOffset;

	public long getWriteOffset() {
		return writeOffset;
	}

	private final File backingFile;
	private HintFile hintFile;
	final int fileId;

	private final HaloDBOptions options;

	private long unFlushedData = 0;

	private HaloDBFile(int fileId, File backingFile, HintFile hintFile, FileChannel writeChannel,
					   FileChannel readChannel, HaloDBOptions options) throws IOException {
		this.fileId = fileId;
		this.backingFile = backingFile;
		this.hintFile = hintFile;
		this.writeChannel = writeChannel;
		this.readChannel = readChannel;
		this.writeOffset = readChannel.size();
		this.options = options;
	}
	
	public Record read(long offset, int length) throws IOException {
		Record record = readRecord(offset);
		assert length == record.getRecordSize();

		return record;
	}

	private int readFromFile(long position, ByteBuffer destinationBuffer) throws IOException {

		long currentPosition = position;
		int bytesRead;
		do {
			bytesRead = readChannel.read(destinationBuffer, currentPosition);
			currentPosition += bytesRead;
		} while (bytesRead != -1 && destinationBuffer.hasRemaining());

		return (int)(currentPosition - position);
	}

	public Record readRecord(long offset) throws IOException {
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
		record.setRecordMetaData(new RecordMetaDataForCache(fileId, offset, header.getRecordSize()));
		return record;
	}

	public RecordMetaDataForCache writeRecord(Record record) throws IOException {

		long start = System.nanoTime();
		writeToChannel(record.serialize(), writeChannel);

		int recordSize = record.getRecordSize();
		long recordOffset = writeOffset;
		writeOffset += recordSize;

		HintFileEntry hintFileEntry = new HintFileEntry(record.getKey(), recordSize, recordOffset, record.getFlags());
		hintFile.write(hintFileEntry);

		HaloDB.recordWriteLatency(System.nanoTime() - start);

		return new RecordMetaDataForCache(fileId, recordOffset, recordSize);
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

	public FileChannel getWriteChannel() {
		return writeChannel;
	}

	public HintFile getHintFile() {
		return hintFile;
	}

	public FileChannel getReadChannel() {
		return readChannel;
	}

	public static HaloDBFile openForReading(File haloDBDirectory, File filename, HaloDBOptions options) throws IOException {

		int fileId = HaloDBFile.getFileTimeStamp(filename);
		
		FileChannel rch = new RandomAccessFile(filename, "r").getChannel();

		HintFile hintFile = new HintFile(fileId, haloDBDirectory, options);
		hintFile.open();

		return new HaloDBFile(fileId, filename, hintFile, null, rch, options);
	}

	static HaloDBFile create(File haloDBDirectory, int fileId, HaloDBOptions options) throws IOException {
		boolean created = false;

		File filename = null;
		while (!created) {
			filename = getDataFile(haloDBDirectory, fileId);
			created = filename.createNewFile();
			if (!created) {
				fileId += 1;
			}
		}

		//TODO: do we need a separate read and write channel.
		RandomAccessFile file = new RandomAccessFile(filename, "rw");
		FileChannel wch = file.getChannel();
		FileChannel rch = new RandomAccessFile(filename, "r").getChannel();

		//TODO: setting the length might improve performance.
		//file.setLength(max_);

		HintFile hintFile = new HintFile(fileId, haloDBDirectory, options);
		hintFile.open();

		return new HaloDBFile(fileId, filename, hintFile, wch, rch, options);
	}

	public HaloDBFileIterator newIterator() throws IOException {
		return new HaloDBFileIterator();
	}

	public synchronized void closeForWriting() throws IOException {
		if (writeChannel != null) {
			writeChannel.close();
			writeChannel = null;
		}
	}

	public synchronized void close() throws IOException {
		closeForWriting();
		if (readChannel != null) {
			readChannel.close();
		}
	}

	public synchronized void delete() throws IOException {
		close();
		if (backingFile != null)
			backingFile.delete();

		if (hintFile != null)
			hintFile.delete();
	}

	private static File getDataFile(File haloDBDirectory, int fileId) {
		return Paths.get(haloDBDirectory.getPath(), fileId + ".data").toFile();
	}

	public static int getFileTimeStamp(File file) {
		Matcher matcher = HaloDBInternal.DATA_FILE_PATTERN.matcher(file.getName());
		matcher.find();
		String s = matcher.group(1);
		return Integer.parseInt(s);
	}


	//TODO: we need to return only fresh files.
	//TODO: scan Hint file iterator for performance.
	public class HaloDBFileIterator implements Iterator<Record> {

		private final long endOffset;
		private long currentOffset = 0;

		HaloDBFileIterator() throws IOException {
			this.endOffset = readChannel.size();
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
