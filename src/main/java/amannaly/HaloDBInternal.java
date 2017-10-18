package amannaly;

import com.google.protobuf.ByteString;

import org.HdrHistogram.Histogram;
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
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import amannaly.ohc.OffHeapCache;

class HaloDBInternal {

    private static final Logger logger = LoggerFactory.getLogger(HaloDBInternal.class);

    private static final Histogram writeLatencyHistogram = new Histogram(TimeUnit.SECONDS.toNanos(5), 3);

    private File dbDirectory;

    private HaloDBFile currentWriteFile;

    private Map<Integer, HaloDBFile> readFileMap = new ConcurrentHashMap<>();

    HaloDBOptions options;



    private KeyCache keyCache;

    private final Map<Integer, Long> staleDataPerFileMap = new ConcurrentHashMap<>();

    private MergeJobThread mergeJobThread;

    private final Set<Integer> filesToMerge = ConcurrentHashMap.newKeySet();

    private HaloDBInternal() {

    }

    static HaloDBInternal open(File directory, HaloDBOptions options) throws IOException {
        HaloDBInternal result = new HaloDBInternal();

        FileUtils.createDirectory(directory);

        result.dbDirectory = directory;

        result.keyCache = new OffHeapCache();
        result.buildReadFileMap();
        result.scanKeyFiles(result.listHintFiles());

        result.dbDirectory = directory;
        result.options = options;

        result.currentWriteFile = result.createHaloDBFile();

        result.mergeJobThread = new MergeJobThread(result, options.mergeJobIntervalInSeconds);
        result.mergeJobThread.start();

        logger.info("Opened HaloDB {}", directory.getName());
        logger.info("isMergeDisabled - {}", options.isMergeDisabled);
        logger.info("maxFileSize - {}", options.maxFileSize);
        logger.info("mergeJobIntervalInSeconds - {}", options.mergeJobIntervalInSeconds);
        logger.info("mergeThresholdPerFile - {}", options.mergeThresholdPerFile);
        logger.info("mergeThresholdFileNumber - {}", options.mergeThresholdFileNumber);

        return result;
    }

    void close() throws IOException {

        mergeJobThread.stopThread();

        // release?
        keyCache.close();

        for (HaloDBFile file : readFileMap.values()) {
            file.close();
        }

        readFileMap.clear();

        if (currentWriteFile != null) {
            currentWriteFile.close();
        }
    }

    void put(ByteString key, ByteString value) throws IOException {
        currentWriteFile = getCurrentWriteFile(key, value);
        RecordMetaData entry = currentWriteFile.write(key, value);

        updateStaleDataMap(key);

        keyCache.put(key, entry);
    }

    ByteString get(ByteString key) throws IOException {
        RecordMetaData metaData = keyCache.get(key);
        if (metaData == null) {
            return null;
        }

        HaloDBFile readFile = readFileMap.get(metaData.fileId);
        if (readFile == null) {
            throw new IllegalArgumentException("no file for " + metaData.fileId);
        }
        return readFile.read(metaData.offset, metaData.recordSize).getValue();
    }

    private HaloDBFile getCurrentWriteFile(ByteString key, ByteString value) throws IOException {
        int size = key.size() + value.size() + Record.HEADER_SIZE;

        if (currentWriteFile == null ||  currentWriteFile.getWriteOffset() + size > options.maxFileSize) {
            if (currentWriteFile != null) {
                currentWriteFile.closeForWriting();
            }

            currentWriteFile = createHaloDBFile();
        }

        return currentWriteFile;

    }

    private void updateStaleDataMap(ByteString key) {
        RecordMetaData recordMetaData = keyCache.get(key);
        if (recordMetaData != null) {
            long stale = recordMetaData.recordSize;
            long currentStaleSize = staleDataPerFileMap.merge(recordMetaData.fileId, stale, (oldValue, newValue) -> oldValue + newValue);

            HaloDBFile file = readFileMap.get(recordMetaData.fileId);

            if (currentStaleSize >= file.getSize() * options.mergeThresholdPerFile) {
                filesToMerge.add(recordMetaData.fileId);
                staleDataPerFileMap.remove(recordMetaData.fileId);
            }
        }
    }

    public boolean areThereEnoughFilesToMerge() {
        return filesToMerge.size() >= options.mergeThresholdFileNumber;
    }

    public Set<Integer> getFilesToMerge() {
        Set<Integer> fileIds = new HashSet<>();
        Iterator<Integer> it = filesToMerge.iterator();

        while (fileIds.size() < options.mergeThresholdFileNumber) {
            fileIds.add(it.next());
        }

        return fileIds;
    }

    public void submitMergedFiles(Set<Integer> mergedFiles) {
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
            result.add(HaloDBFile.openForReading(f, options));
        }

        return result;
    }

    private void buildReadFileMap() throws IOException {
        getHaloDBDataFilesForReading().forEach(f -> readFileMap.put(f.fileId, f));
    }

    //TODO: probably don't expose this?
    //TODO: current we need this for unit testing.
    public final  static Pattern DATA_FILE_PATTERN = Pattern.compile("([0-9]+).data");
    public Set<Integer> listDataFileIds() {
        return new HashSet<>(readFileMap.keySet());
    }


    public static final Pattern HINT_FILE_PATTERN = Pattern.compile("([0-9]+).hint");
    private List<Integer> listHintFiles() {

        File[] files = dbDirectory.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return HINT_FILE_PATTERN.matcher(file.getName()).matches();
            }
        });

        // sort in descending order. we want the latest hint files to be processed first.

        return
        Arrays.stream(files)
            .sorted((f1, f2) -> f2.getName().compareTo(f1.getName()))
            .map(file -> HINT_FILE_PATTERN.matcher(file.getName()))
            .map(matcher -> {
                matcher.find();
                return matcher.group(1);
            })
            .map(Integer::valueOf)
            .collect(Collectors.toList());
    }

    void scanKeyFiles(List<Integer> fileIds) throws IOException {

        logger.info("About to scan {} key files to construct cache\n", fileIds.size());

        long start = System.currentTimeMillis();

        for (int fileId : fileIds) {
            HintFile hintFile = new HintFile(fileId, dbDirectory);
            hintFile.open();
            HintFile.HintFileIterator iterator = hintFile.newIterator();

            while (iterator.hasNext()) {
                HintFileEntry hintFileEntry = iterator.next();
                ByteString key = hintFileEntry.getKey();
                long recordOffset = hintFileEntry.getRecordOffset();
                int recordSize = hintFileEntry.getRecordSize();

                RecordMetaData existing = keyCache.get(key);

                if (existing == null || existing.fileId == fileId) {
                    keyCache.put(key, new RecordMetaData(fileId, recordOffset, recordSize));
                }
                else {
                    //TODO: stale record, add the stale file map to remove later.
                }
            }

            hintFile.close();
        }


        long time = (System.currentTimeMillis() - start)/1000;

        logger.info("Completed scanning all key files in {}.\n", time);
    }

    HaloDBFile getHaloDBFile(int fileId) {
        return readFileMap.get(fileId);
    }

    public void deleteHaloDBFile(int fileId) throws IOException {
        HaloDBFile file = readFileMap.get(fileId);

        if (file != null) {
            readFileMap.remove(fileId);
            file.delete();
        }
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
        RecordMetaData metaData = keyCache.get(record.getKey());

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
}
