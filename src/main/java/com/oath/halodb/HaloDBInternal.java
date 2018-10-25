/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Arjun Mannaly
 */
class HaloDBInternal {
    private static final Logger logger = LoggerFactory.getLogger(HaloDBInternal.class);

    private DBDirectory dbDirectory;

    private volatile HaloDBFile currentWriteFile;

    private TombstoneFile currentTombstoneFile;

    private Map<Integer, HaloDBFile> readFileMap = new ConcurrentHashMap<>();

    HaloDBOptions options;

    private InMemoryIndex inMemoryIndex;

    private final Map<Integer, Integer> staleDataPerFileMap = new ConcurrentHashMap<>();

    private CompactionManager compactionManager;

    private AtomicInteger nextFileId;

    private volatile boolean isClosing = false;

    private volatile long statsResetTime = System.currentTimeMillis();

    private FileLock dbLock;

    private final Lock writeLock = new ReentrantLock();

    private static final int maxReadAttempts = 5;

    private volatile long noOfTombstonesCopiedDuringOpen = 0;
    private volatile long noOfTombstonesFoundDuringOpen = 0;
    private volatile long nextSequenceNumber;

    private HaloDBInternal() {}

    static HaloDBInternal open(File directory, HaloDBOptions options) throws HaloDBException, IOException {
        checkIfOptionsAreCorrect(options);

        HaloDBInternal dbInternal = new HaloDBInternal();
        dbInternal.dbDirectory = DBDirectory.open(directory);

        dbInternal.dbLock = dbInternal.getLock();

        dbInternal.options = options;

        int maxFileId = dbInternal.buildReadFileMap();
        dbInternal.nextFileId = new AtomicInteger(maxFileId + 10);

        DBMetaData dbMetaData = new DBMetaData(dbInternal.dbDirectory);
        dbMetaData.loadFromFileIfExists();
        if (dbMetaData.getMaxFileSize() != 0 && dbMetaData.getMaxFileSize() != options.getMaxFileSize()) {
            throw new IllegalArgumentException("File size cannot be changed after db was created. Current size " + dbMetaData.getMaxFileSize());
        }

        if (dbMetaData.isOpen() || dbMetaData.isIOError()) {
            logger.info("DB was not shutdown correctly last time. Files may not be consistent, repairing them.");
            // open flag is true, this might mean that the db was not cleanly closed the last time.
            dbInternal.repairFiles();
        }
        dbMetaData.setOpen(true);
        dbMetaData.setIOError(false);
        dbMetaData.setVersion(Versions.CURRENT_META_FILE_VERSION);
        dbMetaData.setMaxFileSize(options.getMaxFileSize());
        dbMetaData.storeToFile();

        dbInternal.compactionManager = new CompactionManager(dbInternal);

        dbInternal.inMemoryIndex = new InMemoryIndex(
            options.getNumberOfRecords(), options.isUseMemoryPool(),
            options.getFixedKeySize(), options.getMemoryPoolChunkSize()
        );

        long maxSequenceNumber = dbInternal.buildInMemoryIndex(options);
        if (maxSequenceNumber == -1L) {
            dbInternal.nextSequenceNumber = 1;
            logger.info("Didn't find any existing records; initializing max sequence number to 1");
        } else {
            dbInternal.nextSequenceNumber = maxSequenceNumber + 100;
            logger.info("Found max sequence number {}, now starting from {}", maxSequenceNumber, dbInternal.nextSequenceNumber);
        }

        dbInternal.compactionManager.startCompactionThread();

        logger.info("Opened HaloDB {}", directory.getName());
        logger.info("maxFileSize - {}", options.getMaxFileSize());
        logger.info("compactionThresholdPerFile - {}", options.getCompactionThresholdPerFile());

        return dbInternal;
    }

    void close() throws IOException {
        writeLock.lock();
        try {
            isClosing = true;

            try {
                if(!compactionManager.stopCompactionThread())
                    setIOErrorFlag();
            } catch (IOException e) {
                logger.error("Error while stopping compaction thread. Setting IOError flag", e);
                setIOErrorFlag();
            }

            if (options.isCleanUpInMemoryIndexOnClose())
                inMemoryIndex.close();

            if (currentWriteFile != null) {
                currentWriteFile.flushToDisk();
                currentWriteFile.getIndexFile().flushToDisk();
                currentWriteFile.close();
            }
            if (currentTombstoneFile != null) {
                currentTombstoneFile.flushToDisk();
                currentTombstoneFile.close();
            }

            for (HaloDBFile file : readFileMap.values()) {
                file.close();
            }

            DBMetaData metaData = new DBMetaData(dbDirectory);
            metaData.loadFromFileIfExists();
            metaData.setOpen(false);
            metaData.storeToFile();

            dbDirectory.close();

            if (dbLock != null) {
                dbLock.close();
            }
        } finally {
            writeLock.unlock();
        }
    }

    boolean put(byte[] key, byte[] value) throws IOException, HaloDBException {
        if (key.length > Byte.MAX_VALUE) {
            throw new HaloDBException("key length cannot exceed " + Byte.MAX_VALUE);
        }

        //TODO: more fine-grained locking is possible. 
        writeLock.lock();
        try {
            Record record = new Record(key, value);
            record.setSequenceNumber(getNextSequenceNumber());
            record.setVersion(Versions.CURRENT_DATA_FILE_VERSION);
            InMemoryIndexMetaData entry = writeRecordToFile(record);
            markPreviousVersionAsStale(key);

            //TODO: implement getAndSet and use the return value for
            //TODO: markPreviousVersionAsStale method.
            return inMemoryIndex.put(key, entry);
        } finally {
            writeLock.unlock();
        }
    }

    byte[] get(byte[] key, int attemptNumber) throws IOException, HaloDBException {
        if (attemptNumber > maxReadAttempts) {
            logger.error("Tried {} attempts but read failed", attemptNumber-1);
            throw new HaloDBException("Tried " + attemptNumber + " attempts but failed.");
        }
        InMemoryIndexMetaData metaData = inMemoryIndex.get(key);
        if (metaData == null) {
            return null;
        }

        HaloDBFile readFile = readFileMap.get(metaData.getFileId());
        if (readFile == null) {
            logger.debug("File {} not present. Compaction job would have deleted it. Retrying ...", metaData.getFileId());
            return get(key, attemptNumber+1);
        }

        try {
            return readFile.readFromFile(metaData.getValueOffset(), metaData.getValueSize());
        }
        catch (ClosedChannelException e) {
            if (!isClosing) {
                logger.debug("File {} was closed. Compaction job would have deleted it. Retrying ...", metaData.getFileId());
                return get(key, attemptNumber+1);
            }

            // trying to read after HaloDB.close() method called. 
            throw e;
        }
    }

    int get(byte[] key, ByteBuffer buffer) throws IOException {
        InMemoryIndexMetaData metaData = inMemoryIndex.get(key);
        if (metaData == null) {
            return 0;
        }

        HaloDBFile readFile = readFileMap.get(metaData.getFileId());
        if (readFile == null) {
            logger.debug("File {} not present. Compaction job would have deleted it. Retrying ...", metaData.getFileId());
            return get(key, buffer);
        }

        buffer.clear();
        buffer.limit(metaData.getValueSize());

        try {
            int read = readFile.readFromFile(metaData.getValueOffset(), buffer);
            buffer.flip();
            return read;
        }
        catch (ClosedChannelException e) {
            if (!isClosing) {
                logger.debug("File {} was closed. Compaction job would have deleted it. Retrying ...", metaData.getFileId());
                return get(key, buffer);
            }

            // trying to read after HaloDB.close() method called.
            throw e;
        }
    }

    void delete(byte[] key) throws IOException {
        writeLock.lock();
        try {
            InMemoryIndexMetaData metaData = inMemoryIndex.get(key);
            if (metaData != null) {
                //TODO: implement a getAndRemove method in InMemoryIndex.
                inMemoryIndex.remove(key);
                TombstoneEntry entry =
                    new TombstoneEntry(key, getNextSequenceNumber(), -1, Versions.CURRENT_TOMBSTONE_FILE_VERSION);
                rollOverCurrentTombstoneFile(entry);
                currentTombstoneFile.write(entry);
                markPreviousVersionAsStale(key, metaData);
            }
        } finally {
            writeLock.unlock();
        }
    }

    long size() {
        return inMemoryIndex.size();
    }

    void setIOErrorFlag() throws IOException {
        DBMetaData metaData = new DBMetaData(dbDirectory);
        metaData.loadFromFileIfExists();
        metaData.setIOError(true);
        metaData.storeToFile();
    }

    private InMemoryIndexMetaData writeRecordToFile(Record record) throws IOException, HaloDBException {
        rollOverCurrentWriteFile(record);
        return currentWriteFile.writeRecord(record);
    }

    private void rollOverCurrentWriteFile(Record record) throws IOException, HaloDBException {
        int size = record.getKey().length + record.getValue().length + Record.Header.HEADER_SIZE;

        if ((currentWriteFile == null ||  currentWriteFile.getWriteOffset() + size > options.getMaxFileSize()) && !isClosing) {
            if (currentWriteFile != null) {
                currentWriteFile.flushToDisk();
                currentWriteFile.getIndexFile().flushToDisk();
            }
            currentWriteFile = createHaloDBFile(HaloDBFile.FileType.DATA_FILE);
            dbDirectory.syncMetaData();
        }
    }

    private void rollOverCurrentTombstoneFile(TombstoneEntry entry) throws IOException {
        int size = entry.getKey().length + TombstoneEntry.TOMBSTONE_ENTRY_HEADER_SIZE;

        if ((currentTombstoneFile == null || currentTombstoneFile.getWriteOffset() + size > options.getMaxFileSize()) && !isClosing) {
            if (currentTombstoneFile != null) {
                currentTombstoneFile.flushToDisk();
                currentTombstoneFile.close();
            }
            currentTombstoneFile = TombstoneFile.create(dbDirectory, getNextFileId(), options);
            dbDirectory.syncMetaData();
        }
    }

    private void markPreviousVersionAsStale(byte[] key) {
        InMemoryIndexMetaData recordMetaData = inMemoryIndex.get(key);
        if (recordMetaData != null) {
            markPreviousVersionAsStale(key, recordMetaData);
        }
    }

    private void markPreviousVersionAsStale(byte[] key, InMemoryIndexMetaData recordMetaData) {
        int staleRecordSize = Utils.getRecordSize(key.length, recordMetaData.getValueSize());
        addFileToCompactionQueueIfThresholdCrossed(recordMetaData.getFileId(), staleRecordSize);
    }

    void addFileToCompactionQueueIfThresholdCrossed(int fileId, int staleRecordSize) {
        HaloDBFile file = readFileMap.get(fileId);
        if (file == null)
            return;

        int staleSizeInFile = updateStaleDataMap(fileId, staleRecordSize);
        if (staleSizeInFile >= file.getSize() * options.getCompactionThresholdPerFile()) {

            // We don't want to compact the files the writer thread and the compaction thread is currently writing to.
            if (getCurrentWriteFileId() != fileId && compactionManager.getCurrentWriteFileId() != fileId) {
                if(compactionManager.submitFileForCompaction(fileId)) {
                    staleDataPerFileMap.remove(fileId);
                }
            }
        }
    }

    private int updateStaleDataMap(int fileId, int staleDataSize) {
        return staleDataPerFileMap.merge(fileId, staleDataSize, (oldValue, newValue) -> oldValue + newValue);
    }

    void markFileAsCompacted(int fileId) {
        staleDataPerFileMap.remove(fileId);
    }

    InMemoryIndex getInMemoryIndex() {
        return inMemoryIndex;
    }

    HaloDBFile createHaloDBFile(HaloDBFile.FileType fileType) throws IOException {
        HaloDBFile file = HaloDBFile.create(dbDirectory, getNextFileId(), options, fileType);
        if(readFileMap.putIfAbsent(file.getFileId(), file) != null) {
            throw new IOException("Error while trying to create file " + file.getName() + " file with the given id already exists in the map");
        }
        return file;
    }

    private List<HaloDBFile> openDataFilesForReading() throws IOException {
        File[] files = dbDirectory.listDataFiles();

        List<HaloDBFile> result = new ArrayList<>();
        for (File f : files) {
            HaloDBFile.FileType fileType = HaloDBFile.findFileType(f);
            result.add(HaloDBFile.openForReading(dbDirectory, f, fileType, options));
        }

        return result;
    }

    /**
     * Opens data files for reading and creates a map with file id as the key.
     * Also returns the latest file id in the directory which is then used
     * to determine the next file id.
     */
    private int buildReadFileMap() throws HaloDBException, IOException {
        int maxFileId = Integer.MIN_VALUE;

        for (HaloDBFile file : openDataFilesForReading()) {
            if (readFileMap.putIfAbsent(file.getFileId(), file) != null) {
                // There should only be a single file with a given file id.
                throw new HaloDBException("Found duplicate file with id " + file.getFileId());
            }
            maxFileId = Math.max(maxFileId, file.getFileId());
        }

        if (maxFileId == Integer.MIN_VALUE) {
            // no files in the directory. use the current time as the first file id.
            maxFileId = Ints.checkedCast(System.currentTimeMillis() / 1000);
        }
        return maxFileId;
    }

    private int getNextFileId() {
        return nextFileId.incrementAndGet();
    }

    private Optional<HaloDBFile> getLatestDataFile(HaloDBFile.FileType fileType) {
        return readFileMap.values()
            .stream()
            .filter(f -> f.getFileType() == fileType)
            .max(Comparator.comparingInt(HaloDBFile::getFileId));
    }

    private long buildInMemoryIndex(HaloDBOptions options) throws IOException {
        //TODO: probably processing files in descending order is more efficient.
        List<Integer> indexFiles = dbDirectory.listIndexFiles();

        logger.info("About to scan {} index files to construct index ...", indexFiles.size());

        long start = System.currentTimeMillis();
        long maxSequenceNumber = -1l;

        for (int fileId : indexFiles) {
            IndexFile indexFile = new IndexFile(fileId, dbDirectory, options);
            indexFile.open();
            IndexFile.IndexFileIterator iterator = indexFile.newIterator();

            // build the in-memory index by scanning all index files.
            int count = 0, inserted = 0;
            while (iterator.hasNext()) {
                IndexFileEntry indexFileEntry = iterator.next();
                byte[] key = indexFileEntry.getKey();
                int recordOffset = indexFileEntry.getRecordOffset();
                int recordSize = indexFileEntry.getRecordSize();
                long sequenceNumber = indexFileEntry.getSequenceNumber();
                maxSequenceNumber = Long.max(sequenceNumber, maxSequenceNumber);
                int valueOffset = Utils.getValueOffset(recordOffset, key);
                int valueSize = recordSize - (Record.Header.HEADER_SIZE + key.length);
                count++;

                InMemoryIndexMetaData existing = inMemoryIndex.get(key);

                if (existing == null) {
                    // first version of the record that we have seen, add to index.
                    inMemoryIndex.put(key, new InMemoryIndexMetaData(fileId, valueOffset, valueSize, sequenceNumber));
                    inserted++;
                }
                else if (existing.getSequenceNumber() < sequenceNumber) {
                    // a newer version of the record, replace existing record in index with newer one.
                    inMemoryIndex.put(key, new InMemoryIndexMetaData(fileId, valueOffset, valueSize, sequenceNumber));

                    // update stale data map for the previous version.
                    addFileToCompactionQueueIfThresholdCrossed(existing.getFileId(), Utils.getRecordSize(key.length, existing.getValueSize()));
                    inserted++;
                }
                else {
                    // stale data, update stale data map.
                    addFileToCompactionQueueIfThresholdCrossed(fileId, recordSize);
                }
            }
            logger.debug("Completed scanning index file {}. Found {} records, inserted {} records", fileId, count, inserted);
            indexFile.close();
        }

        // Scan all the tombstone files and remove records from index.
        File[] tombStoneFiles = dbDirectory.listTombstoneFiles();
        logger.info("About to scan {} tombstone files ...", tombStoneFiles.length);
        for (File file : tombStoneFiles) {
            TombstoneFile tombstoneFile = new TombstoneFile(file, options, dbDirectory);
            tombstoneFile.open();
            TombstoneFile.TombstoneFileIterator iterator = tombstoneFile.newIterator();

            int count = 0, deleted = 0, copied = 0;
            while (iterator.hasNext()) {
                TombstoneEntry entry = iterator.next();
                byte[] key = entry.getKey();
                long sequenceNumber = entry.getSequenceNumber();
                maxSequenceNumber = Long.max(sequenceNumber, maxSequenceNumber);
                count++;

                InMemoryIndexMetaData existing = inMemoryIndex.get(key);
                if (existing != null && existing.getSequenceNumber() < sequenceNumber) {
                    // Found a tombstone record which happened after the version currently in index; remove.
                    inMemoryIndex.remove(key);

                    // update stale data map for the previous version.
                    addFileToCompactionQueueIfThresholdCrossed(existing.getFileId(), Utils.getRecordSize(key.length, existing.getValueSize()));
                    deleted++;

                    if (options.isCleanUpTombstonesDuringOpen()) {
                        rollOverCurrentTombstoneFile(entry);
                        dbDirectory.syncMetaData();
                        currentTombstoneFile.write(entry);
                        copied++;
                    }
                }
            }
            logger.debug("Completed scanning tombstone file {}. Found {} tombstones, deleted {} records", file.getName(), count, deleted);
            tombstoneFile.close();

            if (options.isCleanUpTombstonesDuringOpen()) {
                logger.info("Copied {} tombstones from {}. Deleting the file", copied, tombstoneFile.getName());
                if (currentTombstoneFile != null)
                    currentTombstoneFile.flushToDisk();
                tombstoneFile.delete();
            }
            noOfTombstonesCopiedDuringOpen += copied;
            noOfTombstonesFoundDuringOpen += count;
        }

        logger.info("Completed scanning all key files in {}", (System.currentTimeMillis() - start)/1000);

        return maxSequenceNumber;
    }

    HaloDBFile getHaloDBFile(int fileId) {
        return readFileMap.get(fileId);
    }

    void deleteHaloDBFile(int fileId) throws IOException {
        HaloDBFile file = readFileMap.get(fileId);

        if (file != null) {
            readFileMap.remove(fileId);
            file.delete();
        }

        staleDataPerFileMap.remove(fileId);
    }

    private void repairFiles() {
        getLatestDataFile(HaloDBFile.FileType.DATA_FILE).ifPresent(file -> {
            try {
                logger.info("Repairing file {}.data", file.getFileId());
                HaloDBFile repairedFile = file.repairFile(dbDirectory);
                readFileMap.put(repairedFile.getFileId(), repairedFile);
            }
            catch (IOException e) {
                throw new RuntimeException("Exception while repairing data file " + file.getFileId() + " which might be corrupted", e);
            }
        });
        getLatestDataFile(HaloDBFile.FileType.COMPACTED_FILE).ifPresent(file -> {
            try {
                logger.info("Repairing file {}.datac", file.getFileId());
                HaloDBFile repairedFile = file.repairFile(dbDirectory);
                readFileMap.put(repairedFile.getFileId(), repairedFile);
            }
            catch (IOException e) {
                throw new RuntimeException("Exception while repairing datac file " + file.getFileId() + " which might be corrupted", e);
            }
        });

        File[] tombstoneFiles = dbDirectory.listTombstoneFiles();
        if (tombstoneFiles != null && tombstoneFiles.length > 0) {
            TombstoneFile lastFile = new TombstoneFile(tombstoneFiles[tombstoneFiles.length-1], options, dbDirectory);
            try {
                logger.info("Repairing {} file", lastFile.getName());
                lastFile.open();
                TombstoneFile repairedFile = lastFile.repairFile(dbDirectory);
                repairedFile.close();
            } catch (IOException e) {
                throw new RuntimeException("Exception while repairing tombstone file " + lastFile.getName() + " which might be corrupted", e);
            }
        }
    }

    private FileLock getLock() throws HaloDBException {
        try {
            FileLock lock = FileChannel.open(dbDirectory.getPath().resolve("LOCK"), StandardOpenOption.CREATE, StandardOpenOption.WRITE).tryLock();
            if (lock == null) {
                logger.error("Error while opening db. Another process already holds a lock to this db.");
                throw new HaloDBException("Another process already holds a lock for this db.");
            }

            return lock;
        }
        catch (OverlappingFileLockException e) {
            logger.error("Error while opening db. Another process already holds a lock to this db.");
            throw new HaloDBException("Another process already holds a lock for this db.");
        }
        catch (IOException e) {
            logger.error("Error while trying to get a lock on the db.", e);
            throw new HaloDBException("Error while trying to get a lock on the db.", e);
        }
    }

    DBDirectory getDbDirectory() {
        return dbDirectory;
    }

    Set<Integer> listDataFileIds() {
        return new HashSet<>(readFileMap.keySet());
    }

    boolean isRecordFresh(byte[] key, InMemoryIndexMetaData metaData) {
        InMemoryIndexMetaData currentMeta = inMemoryIndex.get(key);

        return
            currentMeta != null
            &&
            metaData.getFileId() == currentMeta.getFileId()
            &&
            metaData.getValueOffset() == currentMeta.getValueOffset();
    }

    private long getNextSequenceNumber() {
        return nextSequenceNumber++;
    }

    private int getCurrentWriteFileId() {
        return currentWriteFile != null ? currentWriteFile.getFileId() : -1;
    }

    private static void checkIfOptionsAreCorrect(HaloDBOptions options) {
        if (options.isUseMemoryPool() && (options.getFixedKeySize() < 0 || options.getFixedKeySize() > Byte.MAX_VALUE)) {
            throw new IllegalArgumentException("fixedKeySize must be set and should be less than 128 when using memory pool");
        }
    }

    boolean isClosing() {
        return isClosing;
    }

    HaloDBStats stats() {
        OffHeapHashTableStats stats = inMemoryIndex.stats();
        return new HaloDBStats(
            statsResetTime,
            stats.getSize(),
            compactionManager.noOfFilesPendingCompaction(),
            computeStaleDataMapForStats(),
            stats.getRehashCount(),
            inMemoryIndex.getNoOfSegments(),
            inMemoryIndex.getMaxSizeOfEachSegment(),
            stats.getSegmentStats(),
            noOfTombstonesFoundDuringOpen,
            options.isCleanUpTombstonesDuringOpen() ? noOfTombstonesFoundDuringOpen - noOfTombstonesCopiedDuringOpen : 0,
            compactionManager.getNumberOfRecordsCopied(),
            compactionManager.getNumberOfRecordsReplaced(),
            compactionManager.getNumberOfRecordsScanned(),
            compactionManager.getSizeOfRecordsCopied(),
            compactionManager.getSizeOfFilesDeleted(),
            compactionManager.getSizeOfFilesDeleted()-compactionManager.getSizeOfRecordsCopied(),
            compactionManager.getCompactionJobRateSinceBeginning(),
            options.clone()
        );
    }

    synchronized void resetStats() {
        inMemoryIndex.resetStats();
        compactionManager.resetStats();
        statsResetTime = System.currentTimeMillis();
    }

    private Map<Integer, Double> computeStaleDataMapForStats() {
        Map<Integer, Double> stats = new HashMap<>();
        staleDataPerFileMap.forEach((fileId, staleData) -> {
            HaloDBFile file = readFileMap.get(fileId);
            if (file != null && file.getSize() > 0) {
                double stalePercent = (1.0*staleData/file.getSize()) * 100;
                stats.put(fileId, stalePercent);
            }
        });

        return stats;
    }

    // Used only in tests.
    @VisibleForTesting
    boolean isCompactionComplete() {
        return compactionManager.isCompactionComplete();
    }
}
