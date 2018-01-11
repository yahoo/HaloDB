package amannaly;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author Arjun Mannaly
 */
public class HaloDB {

	private HaloDBInternal dbInternal;

	//TODO: accept a string instead of File.
	public static HaloDB open(File dirname, HaloDBOptions opts) throws IOException {
		HaloDB db = new HaloDB();
		db.dbInternal = HaloDBInternal.open(dirname, opts);
		return db;
	}

	public byte[] get(byte[] key) throws IOException {
		return dbInternal.get(key);
	}
	

	/**
	 * Reads value into the given destination buffer.
	 * The buffer will be cleared and data will be written
	 * from position 0.
	 */
	public int get(byte[] key, ByteBuffer destination) throws IOException {
		return dbInternal.get(key, destination);
	}

	public void put(byte[] key, byte[] value) throws IOException {
		dbInternal.put(key, value);
	}

	public void delete(byte[] key) throws IOException {
		dbInternal.delete(key);
	}

	public void close() throws IOException {
		dbInternal.close();
	}

	public long size() {
		return dbInternal.size();
	}

	public String stats() {
		return dbInternal.stats();
	}

	public HaloDBIterator newIterator() throws IOException {
		return new HaloDBIterator();
	}

	public class HaloDBIterator implements Iterator<Record> {
		private Iterator<Integer> outer;
		private Iterator<Record> inner;

		private Record next;

		private final List<HaloDBFile.HaloDBFileIterator> files = new ArrayList<>();

		public HaloDBIterator() throws IOException {
			outer = dbInternal.listDataFileIds().iterator();
			if (outer.hasNext()) {
				inner = dbInternal.getHaloDBFile(outer.next()).newIterator();
			}
		}

		@Override
		public boolean hasNext() {
			if (inner == null)
				return false;

			if (next != null)
				return true;

			while (inner.hasNext()) {
				next = inner.next();
				if (dbInternal.isRecordFresh(next)) {
					return true;
				}
			}

			while (outer.hasNext()) {
				try {
					inner = dbInternal.getHaloDBFile(outer.next()).newIterator();
					while (inner.hasNext()) {
						next = inner.next();
						if (dbInternal.isRecordFresh(next)) {
							return true;
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			return false;
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
	}

	// used in tests.
	boolean isMergeComplete() {
		return dbInternal.isMergeComplete();
	}


	//TODO: probably don't expose these methods, used to unit tests.
	Set<Integer> listDataFileIds() {
		return dbInternal.listDataFileIds();
	}
	HaloDBInternal getDbInternal() {
		return dbInternal;
	}
}
