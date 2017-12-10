package amannaly;

import amannaly.cache.ByteArraySerializer;
import amannaly.cache.SequenceNumberSerializer;
import org.HdrHistogram.Histogram;
import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import amannaly.cache.OffHeapCache;

/**
 * @author Arjun Mannaly
 */
class HaloDBInternal {

    private static final Logger logger = LoggerFactory.getLogger(HaloDBInternal.class);

    private static final Histogram writeLatencyHistogram = new Histogram(TimeUnit.SECONDS.toNanos(5), 3);

    private File dbDirectory;

    private volatile HaloDBFile currentWriteFile;

    private Map<Integer, HaloDBFile> readFileMap = new ConcurrentHashMap<>();

    HaloDBOptions options;

    private KeyCache keyCache;

    private final Map<Integer, Integer> staleDataPerFileMap = new ConcurrentHashMap<>();

    private CompactionManager compactionManager;

    private final Set<Integer> filesToMerge = new ConcurrentSkipListSet<>();

    private HaloDBInternal() {}

    static HaloDBInternal open(File directory, HaloDBOptions options) throws IOException {
        HaloDBInternal result = new HaloDBInternal();

        FileUtils.createDirectory(directory);

        result.dbDirectory = directory;

        result.keyCache = new OffHeapCache(options.numberOfRecords);
        result.buildReadFileMap();
        result.buildKeyCache(options);

        result.options = options;
        result.currentWriteFile = result.createHaloDBFile();

        result.compactionManager = new CompactionManager(result, options.mergeJobIntervalInSeconds);
        result.compactionManager.start();

        logger.info("Opened HaloDB {}", directory.getName());
        logger.info("isMergeDisabled - {}", options.isMergeDisabled);
        logger.info("maxFileSize - {}", options.maxFileSize);
        logger.info("mergeJobIntervalInSeconds - {}", options.mergeJobIntervalInSeconds);
        logger.info("mergeThresholdPerFile - {}", options.mergeThresholdPerFile);
        logger.info("mergeThresholdFileNumber - {}", options.mergeThresholdFileNumber);

        return result;
    }

    void close() throws IOException {

        compactionManager.stopThread();
        keyCache.close();

        for (HaloDBFile file : readFileMap.values()) {
            file.close();
        }

        readFileMap.clear();

        if (currentWriteFile != null) {
            currentWriteFile.close();
        }
    }

    void put(byte[] key, byte[] value) throws IOException {
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

        //TODO: there is a potential race condition here as the merge
        // thread could have deleted the file.
        HaloDBFile readFile = readFileMap.get(metaData.fileId);
        if (readFile == null) {
            throw new IllegalArgumentException("no file for " + metaData.fileId);
        }
        return readFile.read(metaData.offset, metaData.recordSize).getValue();
    }

    void delete(byte[] key) throws IOException {
        if (!keyCache.containsKey(key)) {
            return;
        }

        Record record = new Record(key, Record.TOMBSTONE_VALUE);
        record.markAsTombStone();
        record.setSequenceNumber(getNextSequenceNumber());
        writeRecordToFile(record);

        updateStaleDataMap(key);

        keyCache.remove(key);
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
            if (currentWriteFile != null) {
                currentWriteFile.closeForWriting();
            }

            currentWriteFile = createHaloDBFile();
        }
    }

    private void updateStaleDataMap(byte[] key) {
        RecordMetaDataForCache recordMetaData = keyCache.get(key);
        if (recordMetaData != null) {
            int stale = recordMetaData.recordSize;
            long currentStaleSize = staleDataPerFileMap.merge(recordMetaData.fileId, stale, (oldValue, newValue) -> oldValue + newValue);

            HaloDBFile file = readFileMap.get(recordMetaData.fileId);

            if (currentStaleSize >= file.getSize() * options.mergeThresholdPerFile) {
                filesToMerge.add(recordMetaData.fileId);
                staleDataPerFileMap.remove(recordMetaData.fileId);
            }
        }
    }

    boolean areThereEnoughFilesToMerge() {
        //TODO: size() is not a constant time operation.
        //TODO: probably okay since number of files are usually
        //TODO: not too many.
        return filesToMerge.size() >= options.mergeThresholdFileNumber;
    }

    Set<Integer> getFilesToMerge() {
        Set<Integer> fileIds = new HashSet<>();
        Iterator<Integer> it = filesToMerge.iterator();

        //TODO: there was a bug where currentWriteFile was being compacted.
        //TODO: need to write a unit test for this.
        while (fileIds.size() < options.mergeThresholdFileNumber && it.hasNext()) {
            Integer next = it.next();
            if (currentWriteFile.fileId != next)
                fileIds.add(next);
        }

        return fileIds;
    }

    void submitMergedFiles(Set<Integer> mergedFiles) {
        filesToMerge.removeAll(mergedFiles);
    }


    KeyCache getKeyCache() {
        return keyCache;
    }

    HaloDBFile createHaloDBFile() throws IOException {
        HaloDBFile file = HaloDBFile.create(dbDirectory, generateFileId(), options);
        readFileMap.put(file.fileId, file);
        return file;
    }

    private List<HaloDBFile> getHaloDBDataFilesForReading() throws IOException {
        File[] files = dbDirectory.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return DATA_FILE_PATTERN.matcher(file.getName()).matches();
            }
        });

        List<HaloDBFile> result = new ArrayList<>();
        for (File f : files) {
            result.add(HaloDBFile.openForReading(dbDirectory, f, options));
        }

        return result;
    }

    private void buildReadFileMap() throws IOException {
        getHaloDBDataFilesForReading().forEach(f -> readFileMap.put(f.fileId, f));
    }

    //TODO: probably don't expose this?
    //TODO: current we need this for unit testing.
    final  static Pattern DATA_FILE_PATTERN = Pattern.compile("([0-9]+).data");
    Set<Integer> listDataFileIds() {
        return new HashSet<>(readFileMap.keySet());
    }


    static final Pattern INDEX_FILE_PATTERN = Pattern.compile("([0-9]+).index");
    private List<Integer> listIndexFiles() {

        File[] files = dbDirectory.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return INDEX_FILE_PATTERN.matcher(file.getName()).matches();
            }
        });

        // sort in ascending order. we want the earliest index files to be processed first.

        return
        Arrays.stream(files)
            .sorted((f1, f2) -> f1.getName().compareTo(f2.getName()))
            .map(file -> INDEX_FILE_PATTERN.matcher(file.getName()))
            .map(matcher -> {
                matcher.find();
                return matcher.group(1);
            })
            .map(Integer::valueOf)
            .collect(Collectors.toList());
    }

    //TODO: this probably needs to be moved to another location.
    private void buildKeyCache(HaloDBOptions options) throws IOException {
        List<Integer> fileIds = listIndexFiles();

        logger.info("About to scan {} key files to construct cache\n", fileIds.size());

        long start = System.currentTimeMillis();

        // sequence number is needed only when we initially build the key cache
        // therefore, to reduce key cache's memory footprint we keep sequence numbers
        // in a separate cache which is dropped once key cache is constructed.
        int segmentCount = Runtime.getRuntime().availableProcessors() * 2;
        OHCache<byte[], Long> sequenceNumberCache = OHCacheBuilder.<byte[], Long>newBuilder()
                .keySerializer(new ByteArraySerializer())
                .valueSerializer(new SequenceNumberSerializer())
                .capacity(Long.MAX_VALUE)
                .segmentCount(segmentCount)
                .hashTableSize(options.numberOfRecords/segmentCount)
                .eviction(Eviction.NONE)
                .loadFactor(1)
                .build();

        for (int fileId : fileIds) {
            IndexFile indexFile = new IndexFile(fileId, dbDirectory, options);
            indexFile.open();
            IndexFile.IndexFileIterator iterator = indexFile.newIterator();

            while (iterator.hasNext()) {
                IndexFileEntry indexFileEntry = iterator.next();
                byte[] key = indexFileEntry.getKey();
                long recordOffset = indexFileEntry.getRecordOffset();
                int recordSize = indexFileEntry.getRecordSize();
                long sequenceNumber = indexFileEntry.getSequenceNumber();

                RecordMetaDataForCache existing = keyCache.get(key);
                Long existingSequenceNumber = sequenceNumberCache.get(key);

                if (existing != null &&  existingSequenceNumber != null && sequenceNumber > existingSequenceNumber) {
                    // an entry already exists in the key cache but its sequenceNumber is less than
                    // the one in the current index file, therefore we need to replace the entry
                    // in the key cache.

                    if (indexFileEntry.isTombStone()) {
                        keyCache.remove(key);
                        sequenceNumberCache.put(key, sequenceNumber);
                    }
                    else {
                        keyCache.put(key, new RecordMetaDataForCache(fileId, recordOffset, recordSize));
                        sequenceNumberCache.put(key, sequenceNumber);
                    }
                    staleDataPerFileMap.merge(existing.fileId, existing.recordSize, (oldValue, newValue) -> oldValue + newValue);
                }
                else if (existing == null && !indexFileEntry.isTombStone()) {
                    // there is  no entry in the key cache and the current index file entry is not a tombstone.
                    // therefore, if sequence number of the index file entry is greater than the current one
                    // in the sequenceNumber cache, we add the index file entry into the key cache.

                    if (existingSequenceNumber == null || sequenceNumber > existingSequenceNumber) {
                        keyCache.put(key, new RecordMetaDataForCache(fileId, recordOffset, recordSize));
                        sequenceNumberCache.put(key, sequenceNumber);
                    }
                }
                else if (existing == null && indexFileEntry.isTombStone()) {
                    // tombstone entry and there is no record for the key in the key cache.
                    // we don't need to update the key cache, but we might need to update
                    // the sequence number in the sequence cache because a compaction job
                    // might have moved an older version of the record to a newer file.

                    if (existingSequenceNumber == null || sequenceNumber > existingSequenceNumber) {
                        sequenceNumberCache.put(key, sequenceNumber);
                    }
                }
            }

            indexFile.close();
        }

        sequenceNumberCache.close();

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

    public void printStaleFileStatus() {
        System.out.println("********************************");
        staleDataPerFileMap.forEach((key, value) -> {
            System.out.printf("%d.data - %f\n", key, (value * 100.0) / options.maxFileSize);
        });
        System.out.println("********************************\n\n");
    }

    public static void recordWriteLatency(long time) {
        writeLatencyHistogram.recordValue(time);
    }

    public static void printWriteLatencies() {
        System.out.printf("Write latency mean %f\n", writeLatencyHistogram.getMean());
        System.out.printf("Write latency max %d\n", writeLatencyHistogram.getMaxValue());
        System.out.printf("Write latency 99 %d\n", writeLatencyHistogram.getValueAtPercentile(99.0));
        System.out.printf("Write latency total count %d\n", writeLatencyHistogram.getTotalCount());

    }

    public void printKeyCachePutLatencies() {
        keyCache.printPutLatency();
    }

    public boolean isRecordFresh(Record record) {
        RecordMetaDataForCache metaData = keyCache.get(record.getKey());

        return
            metaData != null
            &&
            metaData.fileId == record.getRecordMetaData().fileId
            &&
            metaData.offset == record.getRecordMetaData().offset;

    }

    // File id is the timestamp.
    private int generateFileId() {
        return (int) (System.currentTimeMillis() / 1000L);
    }

    String stats() {
        return keyCache.stats().toString();
    }

    boolean isMergeComplete() {
        return filesToMerge.isEmpty();
    }

    long getNextSequenceNumber() {
        return System.nanoTime();
    }
}
