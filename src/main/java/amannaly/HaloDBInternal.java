package amannaly;

import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
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

    private final Set<Integer> filesToMerge = new ConcurrentSkipListSet<>();

    private AtomicInteger nextFileId;

    private volatile boolean isClosing = false;

    private HaloDBInternal() {}

    static HaloDBInternal open(File directory, HaloDBOptions options) throws IOException {
        HaloDBInternal result = new HaloDBInternal();

        FileUtils.createDirectoryIfNotExists(directory);
        result.dbDirectory = directory;
        result.options = options;

        int maxFileId = result.buildReadFileMap();

        DBMetaData dbMetaData = new DBMetaData(directory.getPath());
        dbMetaData.loadFromFile();
        if (dbMetaData.isOpen()) {
            logger.info("DB was not shutdown correctly last time. Files may not be consistent, repairing them.");
            // open flag is true, this might mean that the db was not cleanly closed the last time.
            repairFiles(result);
        }
        else {
            dbMetaData.setOpen(true);
            dbMetaData.storeToFile();
        }

        result.keyCache = new OffHeapCache(options.numberOfRecords);
        result.nextFileId = new AtomicInteger(maxFileId + 10);
        result.buildKeyCache(options);

        result.currentWriteFile = result.createHaloDBFile(HaloDBFile.FileType.DATA_FILE);

        result.compactionManager = new CompactionManager(result, options.mergeJobIntervalInSeconds);
        result.compactionManager.start();

        result.tombstoneFile = TombstoneFile.create(directory, result.getNextFileId(), options);

        logger.info("Opened HaloDB {}", directory.getName());
        logger.info("isMergeDisabled - {}", options.isMergeDisabled);
        logger.info("maxFileSize - {}", options.maxFileSize);
        logger.info("mergeJobIntervalInSeconds - {}", options.mergeJobIntervalInSeconds);
        logger.info("mergeThresholdPerFile - {}", options.mergeThresholdPerFile);
        logger.info("mergeThresholdFileNumber - {}", options.mergeThresholdFileNumber);

        return result;
    }

    void close() throws IOException {
        isClosing = true;
        compactionManager.stopThread();

        //TODO: make this optional as it will take time.
        keyCache.close();

        for (HaloDBFile file : readFileMap.values()) {
            file.close();
        }

        readFileMap.clear();

        if (currentWriteFile != null) {
            currentWriteFile.close();
        }
        if (tombstoneFile != null) {
            tombstoneFile.close();
        }

        DBMetaData metaData = new DBMetaData(dbDirectory.getPath());
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
        updateStaleDataMap(key);
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
        if (keyCache.remove(key)) {
            TombstoneEntry entry = new TombstoneEntry(key, getNextSequenceNumber());
            rollOverCurrentTombstoneFile(entry);
            tombstoneFile.write(entry);
            updateStaleDataMap(key);
        }
    }

    long size() {
        return keyCache.size();
    }

    private RecordMetaDataForCache writeRecordToFile(Record record) throws IOException {
        rollOverCurrentWriteFile(record);
        return currentWriteFile.writeRecord(record);
    }

    private void rollOverCurrentWriteFile(Record record) throws IOException {
        int size = record.getKey().length + record.getValue().length + Record.Header.HEADER_SIZE;

        if (currentWriteFile == null ||  currentWriteFile.getWriteOffset() + size > options.maxFileSize) {
            currentWriteFile = createHaloDBFile(HaloDBFile.FileType.DATA_FILE);
        }
    }

    private void rollOverCurrentTombstoneFile(TombstoneEntry entry) throws IOException {
        int size = entry.getKey().length + TombstoneEntry.TOMBSTONE_ENTRY_HEADER_SIZE;

        if (tombstoneFile == null ||  tombstoneFile.getWriteOffset() + size > options.maxFileSize) {
            if (tombstoneFile != null) {
                tombstoneFile.close();
            }

            tombstoneFile = TombstoneFile.create(dbDirectory, getNextFileId(), options);
        }
    }

    private void updateStaleDataMap(byte[] key) {
        RecordMetaDataForCache recordMetaData = keyCache.get(key);
        if (recordMetaData != null) {
            int stale = recordMetaData.getValueSize() + key.length + Record.Header.HEADER_SIZE;
            long currentStaleSize = staleDataPerFileMap.merge(recordMetaData.getFileId(), stale, (oldValue, newValue) -> oldValue + newValue);

            HaloDBFile file = readFileMap.get(recordMetaData.getFileId());

            if (currentStaleSize >= file.getSize() * options.mergeThresholdPerFile) {
                filesToMerge.add(recordMetaData.getFileId());
                staleDataPerFileMap.remove(recordMetaData.getFileId());
            }
        }
    }

    boolean areThereEnoughFilesToMerge() {
        //TODO: size() is not a constant time operation.
        //TODO: probably okay since number of files are usually not too many.
        return filesToMerge.size() >= options.mergeThresholdFileNumber;
    }

    int getFileToCompact() {
        for (int fileId : filesToMerge) {
            if (currentWriteFile.fileId != fileId && compactionManager.getCurrentWriteFileId() != fileId) {
                return fileId;
            }
        }

        return -1;
    }

    void submitMergedFile(int fileId) {
        filesToMerge.remove(fileId);
    }

    KeyCache getKeyCache() {
        return keyCache;
    }

    HaloDBFile createHaloDBFile(HaloDBFile.FileType fileType) throws IOException {
        HaloDBFile file = HaloDBFile.create(dbDirectory, getNextFileId(), options, fileType);
        readFileMap.put(file.fileId, file);
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
            readFileMap.put(file.fileId, file);
            maxFileId = Math.max(maxFileId, file.fileId);
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
        List<Integer> indexFiles = FileUtils.listIndexFiles(dbDirectory);

        logger.info("About to scan {} index files to construct cache ...", indexFiles.size());

        long start = System.currentTimeMillis();

        for (int fileId : indexFiles) {
            IndexFile indexFile = new IndexFile(fileId, dbDirectory, options);
            indexFile.open();
            IndexFile.IndexFileIterator iterator = indexFile.newIterator();

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
                    keyCache.put(key, new RecordMetaDataForCache(fileId, valueOffset, valueSize, sequenceNumber));
                    inserted++;
                }
                else if (existing.getSequenceNumber() < sequenceNumber) {
                    keyCache.put(key, new RecordMetaDataForCache(fileId, valueOffset, valueSize, sequenceNumber));
                    int staleDataSize = existing.getValueSize() + key.length + Record.Header.HEADER_SIZE;
                    staleDataPerFileMap.merge(existing.getFileId(), staleDataSize, (oldValue, newValue) -> oldValue + newValue);
                    inserted++;
                }
            }
            logger.debug("Completed scanning index file {}. Found {} records, inserted {} records", fileId, count, inserted);
            indexFile.close();
        }

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
                    keyCache.remove(key);
                    deleted++;
                }
            }
            logger.debug("Completed scanning tombstone file {}. Found {} tombstones, deleted {} records", file.getName(), count, deleted);
            tombstoneFile.close();
        }

        long time = (System.currentTimeMillis() - start)/1000;

        logger.info("Completed scanning all key files in {}.\n", time);
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

    private static void repairFiles(HaloDBInternal db) {
        db.getLatestDataFile(HaloDBFile.FileType.DATA_FILE).ifPresent(file -> {
            try {
                logger.info("Repairing file {}.data", file.getFileId());
                file.repairFile();
            }
            catch (IOException e) {
                throw new RuntimeException("Exception while rebuilding index file " + file.getFileId() + " which might be corrupted", e);
            }
        });
        db.getLatestDataFile(HaloDBFile.FileType.COMPACTED_FILE).ifPresent(file -> {
            try {
                logger.info("Repairing file {}.datac", file.getFileId());
                file.repairFile();
            }
            catch (IOException e) {
                throw new RuntimeException("Exception while rebuilding index file " + file.getFileId() + " which might be corrupted", e);
            }
        });
    }

    Set<Integer> listDataFileIds() {
        return new HashSet<>(readFileMap.keySet());
    }

    void printStaleFileStatus() {
        System.out.println("********************************");
        staleDataPerFileMap.forEach((key, value) -> {
            System.out.printf("%d.data - %f\n", key, (value * 100.0) / options.maxFileSize);
        });
        System.out.println("********************************\n\n");
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

    boolean isMergeComplete() {
        return filesToMerge.isEmpty();
    }

    private long getNextSequenceNumber() {
        return System.nanoTime();
    }
}
