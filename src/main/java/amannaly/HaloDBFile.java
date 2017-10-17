package amannaly;

import com.google.protobuf.ByteString;

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

	private HaloDBFile(int fileId, File backingFile, HintFile hintFile, FileChannel writeChannel,
					   FileChannel readChannel) throws IOException {
		this.fileId = fileId;
		this.backingFile = backingFile;
		this.hintFile = hintFile;
		this.writeChannel = writeChannel;
		this.readChannel = readChannel;
		this.writeOffset = readChannel.size();

	}
	
	public Record read(long offset, int length) throws IOException {
		Record record = readRecord(offset);
		assert length == record.getRecordMetaData().recordSize;

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

		ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
		int readSize = readFromFile(offset, header);
		assert readSize == HEADER_SIZE;
		tempOffset += readSize;

		// read key size and value size from header.
		int keySize = header.getShort(Record.KEY_SIZE_OFFSET);
		int valueSize = header.getInt(Record.VALUE_SIZE_OFFSET);
		int recordSize = HEADER_SIZE + keySize + valueSize;

		ByteBuffer recordBuf = ByteBuffer.allocate(recordSize);
		readFromFile(tempOffset, recordBuf);

		recordBuf.flip();

		ByteString key = ByteString.copyFrom(recordBuf, keySize);
		ByteString value = ByteString.copyFrom(recordBuf, valueSize);

		Record record = new Record(key, value);
		record.setRecordMetaData(new RecordMetaData(fileId, offset, recordSize));
		return record;
	}
	
	public RecordMetaData write(ByteString key, ByteString value) throws IOException {

		long start = System.nanoTime();

		int keySize = key.size();
		int valueSize = value.size();

		int recordSize = HEADER_SIZE + keySize + valueSize;

		writeToChannel(new Record(key, value).serialize(), writeChannel);

		long recordOffset = writeOffset;
		HintFileEntry hintFileEntry = new HintFileEntry(key, recordSize, recordOffset);
		writeOffset += recordSize;
		hintFile.write(hintFileEntry);

		HaloDB.recordWriteLatency(System.nanoTime() - start);

		return new RecordMetaData(fileId, recordOffset, recordSize);
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

	public static HaloDBFile openForReading(File filename) throws IOException {

		int fileId = HaloDBFile.getFileTimeStamp(filename);
		
		FileChannel rch = new RandomAccessFile(filename, "r").getChannel();

		return new HaloDBFile(fileId, filename, null, null, rch);
	}

	static HaloDBFile create(File haloDBDirectory, int fileId) throws IOException {
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

		HintFile hintFile = new HintFile(fileId, haloDBDirectory);
		hintFile.open();

		return new HaloDBFile(fileId, filename, hintFile, wch, rch);
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
