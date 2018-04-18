package com.oath.halodb;

import com.google.common.annotations.VisibleForTesting;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

/**
 * @author Arjun Mannaly
 */
public class HaloDB {

    private HaloDBInternal dbInternal;

    public static HaloDB open(File dirname, HaloDBOptions opts) throws IOException {
        HaloDB db = new HaloDB();
        db.dbInternal = HaloDBInternal.open(dirname, opts);
        return db;
    }

    public static HaloDB open(String directory, HaloDBOptions opts) throws IOException {
        return HaloDB.open(new File(directory), opts);
    }

    public byte[] get(byte[] key) throws IOException {
        return dbInternal.get(key, 1);
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

    public HaloDBStats stats() {
        return dbInternal.stats();
    }

    public void resetStats() {
        dbInternal.resetStats();
    }

    public HaloDBIterator newIterator() throws IOException {
        return new HaloDBIterator(dbInternal);
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
