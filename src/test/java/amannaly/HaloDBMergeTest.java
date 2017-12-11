package amannaly;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Arjun Mannaly
 */
public class HaloDBMergeTest {

    private final int recordSize = 1024;
    private final int numberOfFiles = 8;
    private final int recordsPerFile = 10;
    private final int numberOfRecords = numberOfFiles * recordsPerFile;

    @Test
    public void testMerge() throws Exception {
        File directory = new File("/tmp/HaloDBTestWithMerge/testMerge");
        TestUtils.deleteDirectory(directory);

        HaloDBOptions options = new HaloDBOptions();
        options.maxFileSize = recordsPerFile * recordSize;
        options.mergeThresholdPerFile = 0.5;
        options.mergeThresholdFileNumber = 4;
        options.isMergeDisabled = false;
        options.mergeJobIntervalInSeconds = 2;
        options.flushDataSizeBytes = 2048;

        HaloDB db = HaloDB.open(directory, options);

        Record[] records = insertAndUpdateRecords(numberOfRecords, db);

        // wait for the merge jobs to complete.
        Thread.sleep(10000);

        Map<Long, List<Path>> map = Files.list(directory.toPath())
            .filter(path -> HaloDBInternal.DATA_FILE_PATTERN.matcher(path.getFileName().toString()).matches())
            .collect(Collectors.groupingBy(path -> path.toFile().length()));

        // 4 data files of size 10K.
        Assert.assertEquals(map.get(10 * 1024l).size(), 4);

        //2 merged data files of size 20K.
        Assert.assertEquals(map.get(20 * 1024l).size(), 2);

        int sizeOfIndexEntry = IndexFileEntry.INDEX_FILE_HEADER_SIZE + 8;
        Map<Long, List<Path>> indexFileSizemap = Files.list(directory.toPath())
            .filter(path -> HaloDBInternal.INDEX_FILE_PATTERN.matcher(path.getFileName().toString()).matches())
            .collect(Collectors.groupingBy(path -> path.toFile().length()));

        // 4 index files of size 220 bytes.
        Assert.assertEquals(indexFileSizemap.get(sizeOfIndexEntry * 10l).size(), 4);

        // 2 index files of size 440 bytes.
        Assert.assertEquals(indexFileSizemap.get(sizeOfIndexEntry * 20l).size(), 2);

        for (Record r : records) {
            byte[] actual = db.get(r.getKey());
            Assert.assertArrayEquals("key -> " + Utils.bytesToLong(r.getKey()), actual, r.getValue());
        }

        db.close();

        TestUtils.deleteDirectory(directory);
    }

    @Test
    public void testReOpenDBAfterMerge() throws IOException, InterruptedException {
        File directory = new File("/tmp/HaloDBTestWithMerge/testReOpenDBAfterMerge");
        TestUtils.deleteDirectory(directory);

        HaloDBOptions options = new HaloDBOptions();
        options.maxFileSize = recordsPerFile * recordSize;
        options.mergeThresholdPerFile = 0.5;
        options.mergeThresholdFileNumber = 4;
        options.isMergeDisabled = false;
        options.mergeJobIntervalInSeconds = 2;

        HaloDB db = HaloDB.open(directory, options);

        Record[] records = insertAndUpdateRecords(numberOfRecords, db);

        // wait for the merge jobs to complete.
        Thread.sleep(10000);

        db.close();

        Thread.sleep(5000);

        db = HaloDB.open(directory, options);

        for (Record r : records) {
            byte[] actual = db.get(r.getKey());
            Assert.assertArrayEquals(actual, r.getValue());
        }

        db.close();

        TestUtils.deleteDirectory(directory);
    }

    @Test
    public void testReOpenDBWithoutMerge() throws IOException, InterruptedException {
        File directory = new File("/tmp/HaloDBTestWithMerge/testReOpenAndUpdatesAndWithoutMerge");
        TestUtils.deleteDirectory(directory);

        HaloDBOptions options = new HaloDBOptions();
        options.maxFileSize = recordsPerFile * recordSize;
        options.isMergeDisabled = true;

        HaloDB db = HaloDB.open(directory, options);

        Record[] records = insertAndUpdateRecords(numberOfRecords, db);

        db.close();

        Thread.sleep(2000);

        db = HaloDB.open(directory, options);

        for (Record r : records) {
            byte[] actual = db.get(r.getKey());
            Assert.assertArrayEquals(actual, r.getValue());
        }

        db.close();

        TestUtils.deleteDirectory(directory);
    }

    @Test
    public void testUpdatesToSameFile() throws IOException, InterruptedException {
        File directory = new File("/tmp/HaloDBTestWithMerge/testUpdatesToSameFile");
        TestUtils.deleteDirectory(directory);

        HaloDBOptions options = new HaloDBOptions();
        options.maxFileSize = recordsPerFile * recordSize;
        options.isMergeDisabled = true;

        HaloDB db = HaloDB.open(directory, options);

        Record[] records = insertAndUpdateRecordsToSameFile(2, db);

        db.close();

        Thread.sleep(2000);

        db = HaloDB.open(directory, options);

        for (Record r : records) {
            byte[] actual = db.get(r.getKey());
            Assert.assertArrayEquals(actual, r.getValue());
        }

        db.close();

        TestUtils.deleteDirectory(directory);
    }

    private Record[] insertAndUpdateRecords(int numberOfRecords, HaloDB db) throws IOException {
        int valueSize = recordSize - Record.Header.HEADER_SIZE - 8; // 8 is the key size.

        Record[] records = new Record[numberOfRecords];
        for (int i = 0; i < numberOfRecords; i++) {
            byte[] key = Utils.longToBytes(i);
            byte[] value = TestUtils.generateRandomByteArray(valueSize);
            records[i] = new Record(key, value);
            db.put(records[i].getKey(), records[i].getValue());
        }

        // modify first 5 records of each file.
        byte[] modifiedMark = "modified".getBytes();
        for (int k = 0; k < numberOfFiles; k++) {
            for (int i = 0; i < 5; i++) {
                Record r = records[i + k*10];
                byte[] value = r.getValue();
                System.arraycopy(modifiedMark, 0, value, 0, modifiedMark.length);
                Record modifiedRecord = new Record(r.getKey(), value);
                records[i + k*10] = modifiedRecord;
                db.put(modifiedRecord.getKey(), modifiedRecord.getValue());
            }
        }
        return records;
    }

    private Record[] insertAndUpdateRecordsToSameFile(int numberOfRecords, HaloDB db) throws IOException {
        int valueSize = recordSize - Record.Header.HEADER_SIZE - 8; // 8 is the key size.

        Record[] records = new Record[numberOfRecords];
        for (int i = 0; i < numberOfRecords; i++) {
            byte[] key = Utils.longToBytes(i);
            byte[] value = TestUtils.generateRandomByteArray(valueSize);

            byte[] updatedValue = null;
            for (long j = 0; j < recordsPerFile; j++) {
                updatedValue = TestUtils.concatenateArrays(value, Utils.longToBytes(j));
                db.put(key, updatedValue);
            }

            // only store the last updated valued.
            records[i] = new Record(key, updatedValue);
        }

        return records;
    }
}
