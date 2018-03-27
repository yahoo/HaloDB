package com.oath.halodb;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Arjun Mannaly
 */
class HaloDBInternal {
    private static final Logger logger = LoggerFactory.getLogger(HaloDBInternal.class);

    private File dbDirectory;

    private volatile HaloDBFile currentWriteFile;

    private TombstoneFile tombstoneFile;

    private Map<Integer, HaloDBFile> readFileMap = new ConcurrentHashMap<>();

    HaloDBOptions options;

    private KeyCache keyCache;

    private final Map<Integer, Integer> staleDataPerFileMap = new ConcurrentHashMap<>();

    private CompactionManager compactionManager;

    private AtomicInteger nextFileId;

    private volatile boolean isClosing = false;

    private HaloDBInternal() {}

    static HaloDBInternal open(File directory, HaloDBOptions options) throws IOException {
        HaloDBInternal dbInternal = new HaloDBInternal();

        FileUtils.createDirectoryIfNotExists(directory);
        dbInternal.dbDirectory = directory;
        dbInternal.options = options;

        int maxFileId = dbInternal.buildReadFileMap();
        dbInternal.nextFileId = new AtomicInteger(maxFileId + 10);

        DBMetaData dbMetaData = new DBMetaData(directory.getPath());
        dbMetaData.loadFromFileIfExists();
        if (dbMetaData.isOpen() || dbMetaData.isIOError()) {
            logger.info("DB was not shutdown correctly last time. Files may not be consistent, repairing them.");
            // open flag is true, this might mean that the db was not cleanly closed the last time.
            dbInternal.repairFiles();
        }
        dbMetaData.setOpen(true);
        dbMetaData.setIOError(false);
        dbMetaData.storeToFile();

        dbInternal.compactionManager = new CompactionManager(dbInternal);

        dbInternal.keyCache = new OffHeapCache(options.numberOfRecords);
        dbInternal.buildKeyCache(options);
        dbInternal.compactionManager.startCompactionThread();

        logger.info("Opened HaloDB {}", directory.getName());
        logger.info("isCompactionDisabled - {}", options.isCompactionDisabled);
        logger.info("maxFileSize - {}", options.maxFileSize);
        logger.info("mergeThresholdPerFile - {}", options.mergeThresholdPerFile);

        return dbInternal;
    }

    void close() throws IOException {
        isClosing = true;

        try {
            if(!compactionManager.stopCompactionThread())
                setIOErrorFlag();
        } catch (IOException e) {
            logger.error("Error while stopping compaction thread. Setting IOError flag", e);
            setIOErrorFlag();
        }

        if (options.cleanUpKeyCacheOnClose)
            keyCache.close();

        for (HaloDBFile file : readFileMap.values()) {
            file.close();
        }

        readFileMap.clear();

        if (currentWriteFile != null) {
            currentWriteFile.flushToDisk();
            currentWriteFile.getIndexFile().flushToDisk();
            currentWriteFile.close();
        }
        if (tombstoneFile != null) {
            tombstoneFile.flushToDisk();
            tombstoneFile.close();
        }

        DBMetaData metaData = new DBMetaData(dbDirectory.getPath());
        metaData.loadFromFileIfExists();
        metaData.setOpen(false);
        metaData.storeToFile();
    }

    void put(byte[] key, byte[] value) throws IOException {
        if (key.length > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("key length cannot exceed " + Byte.MAX_VALUE);
        }

        Record record = new Record(key, value);
        record.setSequenceNumber(getNextSequenceNumber());
        RecordMetaDataForCache entry = writeRecordToFile(record);
        markPreviousVersionAsStale(key);

        //TODO: implement getAndSet and use the return value for
        //TODO: markPreviousVersionAsStale method.   
        keyCache.put(key, entry);
    }

    byte[] get(byte[] key) throws IOException {
        RecordMetaDataForCache metaData = keyCache.get(key);
        if (metaData == null) {
            return null;
        }

        HaloDBFile readFile = readFileMap.get(metaData.getFileId());
        if (readFile == null) {
            logger.debug("File {} not present. Compaction job would have deleted it. Retrying ...", metaData.getFileId());
            return get(key);
        }

        try {
            return readFile.readFromFile(metaData.getValueOffset(), metaData.getValueSize());
        }
        catch (ClosedChannelException e) {
            if (!isClosing) {
                logger.debug("File {} was closed. Compaction job would have deleted it. Retrying ...", metaData.getFileId());
                return get(key);
            }

            // trying to read after HaloDB.close() method called. 
            throw e;
        }
    }

    int get(byte[] key, ByteBuffer buffer) throws IOException {
        RecordMetaDataForCache metaData = keyCache.get(key);
        if (metaData == null) {
            return 0;
        }

        HaloDBFile readFile = readFileMap.get(metaData.getFileId());
        if (readFile == null) {
            logger.debug("File {} not present. Merge job would have deleted it. Retrying ...", metaData.getFileId());
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
        RecordMetaDataForCache metaData = keyCache.get(key);
        if (metaData != null) {
            //TODO: implement a getAndRemove method in keyCache. 
            keyCache.remove(key);
            TombstoneEntry entry = new TombstoneEntry(key, getNextSequenceNumber());
            rollOverCurrentTombstoneFile(entry);
            tombstoneFile.write(entry);
            markPreviousVersionAsStale(key, metaData);
        }
    }

    long size() {
        return keyCache.size();
    }

    void setIOErrorFlag() throws IOException {
        DBMetaData metaData = new DBMetaData(dbDirectory.getPath());
        metaData.loadFromFileIfExists();
        metaData.setIOError(true);
        metaData.storeToFile();
    }

    private RecordMetaDataForCache writeRecordToFile(Record record) throws IOException {
        rollOverCurrentWriteFile(record);
        return currentWriteFile.writeRecord(record);
    }

    private void rollOverCurrentWriteFile(Record record) throws IOException {
        int size = record.getKey().length + record.getValue().length + Record.Header.HEADER_SIZE;

        if (currentWriteFile == null ||  currentWriteFile.getWriteOffset() + size > options.maxFileSize) {
            if (currentWriteFile != null) {
                currentWriteFile.flushToDisk();
                currentWriteFile.getIndexFile().flushToDisk();
            }
            currentWriteFile = createHaloDBFile(HaloDBFile.FileType.DATA_FILE);
        }
    }

    private void rollOverCurrentTombstoneFile(TombstoneEntry entry) throws IOException {
        int size = entry.getKey().length + TombstoneEntry.TOMBSTONE_ENTRY_HEADER_SIZE;

        if (tombstoneFile == null ||  tombstoneFile.getWriteOffset() + size > options.maxFileSize) {
            if (tombstoneFile != null) {
                tombstoneFile.flushToDisk();
                tombstoneFile.close();
            }

            tombstoneFile = TombstoneFile.create(dbDirectory, getNextFileId(), options);
        }
    }

    private void markPreviousVersionAsStale(byte[] key) {
        RecordMetaDataForCache recordMetaData = keyCache.get(key);
        if (recordMetaData != null) {
            markPreviousVersionAsStale(key, recordMetaData);
        }
    }

    private void markPreviousVersionAsStale(byte[] key, RecordMetaDataForCache recordMetaData) {
        int staleRecordSize = Utils.getRecordSize(key.length, recordMetaData.getValueSize());
        addFileToCompactionQueueIfThresholdCrossed(recordMetaData.getFileId(), staleRecordSize);
    }

    void addFileToCompactionQueueIfThresholdCrossed(int fileId, int staleRecordSize) {
        HaloDBFile file = readFileMap.get(fileId);
        if (file == null)
            return;

        int staleSizeInFile = updateStaleDataMap(fileId, staleRecordSize);
        if (staleSizeInFile >= file.getSize() * options.mergeThresholdPerFile) {

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

    KeyCache getKeyCache() {
        return keyCache;
    }

    HaloDBFile createHaloDBFile(HaloDBFile.FileType fileType) throws IOException {
        HaloDBFile file = HaloDBFile.create(dbDirectory, getNextFileId(), options, fileType);
        readFileMap.put(file.getFileId(), file);
        return file;
    }

    private List<HaloDBFile> openDataFilesForReading() throws IOException {
        File[] files = FileUtils.listDataFiles(dbDirectory);

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
    private int buildReadFileMap() throws IOException {
        int maxFileId = Integer.MIN_VALUE;

        for (HaloDBFile file : openDataFilesForReading()) {
            if (readFileMap.putIfAbsent(file.getFileId(), file) != null) {
                // There should only be a single file with a given file id.
                throw new IOException("Found duplicate file with id " + file.getFileId());
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

    private void buildKeyCache(HaloDBOptions options) throws IOException {
        //TODO: probably processing files in descending order is more efficient.
        List<Integer> indexFiles = FileUtils.listIndexFiles(dbDirectory);

        logger.info("About to scan {} index files to construct cache ...", indexFiles.size());

        long start = System.currentTimeMillis();

        for (int fileId : indexFiles) {
            IndexFile indexFile = new IndexFile(fileId, dbDirectory, options);
            indexFile.open();
            IndexFile.IndexFileIterator iterator = indexFile.newIterator();

            // build the cache by scanning all index files. 
            int count = 0, inserted = 0;
            while (iterator.hasNext()) {
                IndexFileEntry indexFileEntry = iterator.next();
                byte[] key = indexFileEntry.getKey();
                int recordOffset = indexFileEntry.getRecordOffset();
                int recordSize = indexFileEntry.getRecordSize();
                long sequenceNumber = indexFileEntry.getSequenceNumber();
                int valueOffset = Utils.getValueOffset(recordOffset, key);
                int valueSize = recordSize - (Record.Header.HEADER_SIZE + key.length);
                count++;

                RecordMetaDataForCache existing = keyCache.get(key);

                if (existing == null) {
                    // first version of the record that we have seen, add to cache.
                    keyCache.put(key, new RecordMetaDataForCache(fileId, valueOffset, valueSize, sequenceNumber));
                    inserted++;
                }
                else if (existing.getSequenceNumber() <= sequenceNumber) {
                    // a newer version of the record, replace existing record in cache with newer one.
                    keyCache.put(key, new RecordMetaDataForCache(fileId, valueOffset, valueSize, sequenceNumber));

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

        // Scan all the tombstone files and remove records from cache. 
        File[] tombStoneFiles = FileUtils.listTombstoneFiles(dbDirectory);
        logger.info("About to scan {} tombstone files ...", tombStoneFiles.length);
        for (File file : tombStoneFiles) {
            TombstoneFile tombstoneFile = new TombstoneFile(file, options);
            tombstoneFile.open();
            TombstoneFile.TombstoneFileIterator iterator = tombstoneFile.newIterator();

            int count = 0, deleted = 0;
            while (iterator.hasNext()) {
                TombstoneEntry entry = iterator.next();
                byte[] key = entry.getKey();
                long sequenceNumber = entry.getSequenceNumber();
                count++;

                RecordMetaDataForCache existing = keyCache.get(key);
                if (existing != null && existing.getSequenceNumber() < sequenceNumber) {
                    // Found a tombstone record which happened after the version currently in cache; remove.
                    keyCache.remove(key);

                    // update stale data map for the previous version.
                    addFileToCompactionQueueIfThresholdCrossed(existing.getFileId(), Utils.getRecordSize(key.length, existing.getValueSize()));
                    deleted++;
                }
            }
            logger.debug("Completed scanning tombstone file {}. Found {} tombstones, deleted {} records", file.getName(), count, deleted);
            tombstoneFile.close();
        }

        logger.info("Completed scanning all key files in {}", (System.currentTimeMillis() - start)/1000);
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
                HaloDBFile newFile = file.repairFile(getNextFileId());
                readFileMap.put(newFile.getFileId(), newFile);
                readFileMap.remove(file.getFileId());
            }
            catch (IOException e) {
                throw new RuntimeException("Exception while rebuilding index file " + file.getFileId() + " which might be corrupted", e);
            }
        });
        getLatestDataFile(HaloDBFile.FileType.COMPACTED_FILE).ifPresent(file -> {
            try {
                logger.info("Repairing file {}.datac", file.getFileId());
                HaloDBFile newFile = file.repairFile(getNextFileId());
                readFileMap.put(newFile.getFileId(), newFile);
                readFileMap.remove(file.getFileId());
            }
            catch (IOException e) {
                throw new RuntimeException("Exception while rebuilding index file " + file.getFileId() + " which might be corrupted", e);
            }
        });
    }

    Set<Integer> listDataFileIds() {
        return new HashSet<>(readFileMap.keySet());
    }



    boolean isRecordFresh(byte[] key, RecordMetaDataForCache metaData) {
        RecordMetaDataForCache metaDataFromCache = keyCache.get(key);

        return
            metaDataFromCache != null
            &&
            metaData.getFileId() == metaDataFromCache.getFileId()
            &&
            metaData.getValueOffset() == metaDataFromCache.getValueOffset();
    }

    String stats() {
        return keyCache.stats().toString();
    }

    private long getNextSequenceNumber() {
        return System.nanoTime();
    }

    int getCurrentWriteFileId() {
        return currentWriteFile != null ? currentWriteFile.getFileId() : -1;
    }

    void printStaleFileStatus() {
        logger.info("Stale data per file =>");

        staleDataPerFileMap.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(e -> {
            int fileId = e.getKey();
            int staleSize = e.getValue();
            HaloDBFile file = readFileMap.get(fileId);
            if (file != null)
                logger.info("{} - {}", fileId, (staleSize * 100.0)/file.getSize() );
        });
    }

    boolean isClosing() {
        return isClosing;
    }

    // Used only in tests.
    @VisibleForTesting
    boolean isMergeComplete() {
        return compactionManager.isMergeComplete();
    }
}
