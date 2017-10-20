package amannaly;

import org.HdrHistogram.Histogram;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class HaloDB {

	private static final Histogram writeLatencyHistogram = new Histogram(TimeUnit.SECONDS.toNanos(5), 3);

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

	public void put(byte[] key, byte[] value) throws IOException {
		dbInternal.put(key, value);
	}

	public void delete(byte[] key) throws IOException {
		dbInternal.delete(key);
	}

	public void close() throws IOException {
		dbInternal.close();
	}

	//TODO: probably don't expose this?
	//TODO: current we need this for unit testing.
	public Set<Integer> listDataFileIds() {
		return dbInternal.listDataFileIds();
	}

	//TODO: exposing this doesn't look good, any way around this?
	// Used for unit tests.
	HaloDBInternal getDbInternal() {
		return dbInternal;
	}

	public static void recordWriteLatency(long time) {
		//writeLatencyHistogram.recordValue(time);
	}

	public static void printWriteLatencies() {
		System.out.printf("Write latency mean %f\n", writeLatencyHistogram.getMean());
		System.out.printf("Write latency max %d\n", writeLatencyHistogram.getMaxValue());
		System.out.printf("Write latency 99 %d\n", writeLatencyHistogram.getValueAtPercentile(99.0));
		System.out.printf("Write latency total count %d\n", writeLatencyHistogram.getTotalCount());

	}

	public HaloDBIterator newIterator() throws IOException {
		return new HaloDBIterator();
	}

	//TODO: move logic to check if a file is fresh/stale to file iterator.
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
}
