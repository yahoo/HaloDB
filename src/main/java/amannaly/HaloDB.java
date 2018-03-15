package amannaly;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author Arjun Mannaly
 */
public class HaloDB {
    private static final Logger logger = LoggerFactory.getLogger(HaloDBInternal.class);

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
        private Iterator<IndexFileEntry> inner;
        private HaloDBFile currentFile;

        private Record next;

        public HaloDBIterator() throws IOException {
            outer = dbInternal.listDataFileIds().iterator();
            if (outer.hasNext()) {
                currentFile = dbInternal.getHaloDBFile(outer.next());
                inner = currentFile.getIndexFile().newIterator();
            }
        }

        @Override
        public boolean hasNext() {
            if (inner == null)
                return false;

            if (next != null)
                return true;

            while (inner.hasNext()) {
                IndexFileEntry entry = inner.next();
                try {
                    next = readRecordFromDataFile(entry);
                    if (next != null) {
                        return true;
                    }
                } catch (IOException e) {
                    logger.info("Error in iterator", e);
                    return false;
                }
            }

            while (outer.hasNext()) {
                try {
                    currentFile = dbInternal.getHaloDBFile(outer.next());
                    inner = currentFile.getIndexFile().newIterator();

                    while (inner.hasNext()) {
                        IndexFileEntry entry = inner.next();
                        next = readRecordFromDataFile(entry);
                        if (next != null) {
                            return true;
                        }
                    }
                } catch (IOException e) {
                    logger.info("Error in iterator", e);
                    return false;
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

        private Record readRecordFromDataFile(IndexFileEntry entry) throws IOException {
            RecordMetaDataForCache meta = Utils.getMetaData(entry, currentFile.getFileId());
            Record record = null;
            if (dbInternal.isRecordFresh(entry.getKey(), meta)) {
                byte[] value = currentFile.readFromFile(
                    Utils.getValueOffset(entry.getRecordOffset(), entry.getKey()),
                    Utils.getValueSize(entry.getRecordSize(), entry.getKey()));
                record = new Record(entry.getKey(), value);
                record.setRecordMetaData(meta);
            }
            return record;
        }
    }


    // methods used in tests.

    @VisibleForTesting
    boolean isMergeComplete() {
        return dbInternal.isMergeComplete();
    }
    
    Set<Integer> listDataFileIds() {
        return dbInternal.listDataFileIds();
    }

    HaloDBInternal getDbInternal() {
        return dbInternal;
    }

    public void printStaleFileStatus() {
        dbInternal.printStaleFileStatus();
    }
}
