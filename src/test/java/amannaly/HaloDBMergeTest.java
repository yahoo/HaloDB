package amannaly;

import com.google.common.primitives.Longs;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Arjun Mannaly
 */
public class HaloDBMergeTest extends TestBase {

    private final int recordSize = 1024;
    private final int numberOfFiles = 8;
    private final int recordsPerFile = 10;
    private final int numberOfRecords = numberOfFiles * recordsPerFile;

    @Test
    public void testMerge() throws Exception {
        String directory = "/tmp/HaloDBTestWithMerge/testMerge";

        HaloDBOptions options = new HaloDBOptions();
        options.maxFileSize = recordsPerFile * recordSize;
        options.mergeThresholdPerFile = 0.5;
        options.isMergeDisabled = false;
        options.mergeJobIntervalInSeconds = 2;
        options.flushDataSizeBytes = 2048;

        HaloDB db =  getTestDB(directory, options);

        Record[] records = insertAndUpdateRecords(numberOfRecords, db);

        TestUtils.waitForMergeToComplete(db);

        Map<String, List<Path>> map = Files.list(Paths.get(directory))
            .filter(path -> Constants.DATA_FILE_PATTERN.matcher(path.getFileName().toString()).matches())
            .collect(Collectors.groupingBy(path -> "."+path.toFile().getName().split("\\.")[1]));

        // 4 data files of size.
        Assert.assertEquals(map.get(HaloDBFile.DATA_FILE_NAME).size(), 4);

        //4 merged data files.
        Assert.assertEquals(map.get(HaloDBFile.COMPACTED_DATA_FILE_NAME).size(), 4);

        long actualIndexFileCount = Files.list(Paths.get(directory))
            .filter(path -> Constants.INDEX_FILE_PATTERN.matcher(path.getFileName().toString()).matches())
            .count();

        // 8 index files.
        Assert.assertEquals(actualIndexFileCount, 8);

        for (Record r : records) {
            byte[] actual = db.get(r.getKey());
            Assert.assertEquals(r.getValue(), actual);
        }
    }

    @Test
    public void testReOpenDBAfterMerge() throws IOException, InterruptedException {
        String directory = "/tmp/HaloDBTestWithMerge/testReOpenDBAfterMerge";

        HaloDBOptions options = new HaloDBOptions();
        options.maxFileSize = recordsPerFile * recordSize;
        options.mergeThresholdPerFile = 0.5;
        options.isMergeDisabled = false;
        options.mergeJobIntervalInSeconds = 2;

        HaloDB db = getTestDB(directory, options);

        Record[] records = insertAndUpdateRecords(numberOfRecords, db);

        TestUtils.waitForMergeToComplete(db);

        db.close();

        db = getTestDBWithoutDeletingFiles(directory, options);

        for (Record r : records) {
            byte[] actual = db.get(r.getKey());
            Assert.assertEquals(actual, r.getValue());
        }
    }

    @Test
    public void testReOpenDBWithoutMerge() throws IOException, InterruptedException {
        String directory ="/tmp/HaloDBTestWithMerge/testReOpenAndUpdatesAndWithoutMerge";

        HaloDBOptions options = new HaloDBOptions();
        options.maxFileSize = recordsPerFile * recordSize;
        options.isMergeDisabled = true;

        HaloDB db = getTestDB(directory, options);

        Record[] records = insertAndUpdateRecords(numberOfRecords, db);

        db.close();

        db = getTestDBWithoutDeletingFiles(directory, options);

        for (Record r : records) {
            byte[] actual = db.get(r.getKey());
            Assert.assertEquals(actual, r.getValue());
        }
    }

    @Test
    public void testUpdatesToSameFile() throws IOException, InterruptedException {
        String directory ="/tmp/HaloDBTestWithMerge/testUpdatesToSameFile";

        HaloDBOptions options = new HaloDBOptions();
        options.maxFileSize = recordsPerFile * recordSize;
        options.isMergeDisabled = true;

        HaloDB db = getTestDB(directory, options);

        Record[] records = insertAndUpdateRecordsToSameFile(2, db);

        db.close();

        db = getTestDBWithoutDeletingFiles(directory, options);

        for (Record r : records) {
            byte[] actual = db.get(r.getKey());
            Assert.assertEquals(actual, r.getValue());
        }
    }

    private Record[] insertAndUpdateRecords(int numberOfRecords, HaloDB db) throws IOException {
        int valueSize = recordSize - Record.Header.HEADER_SIZE - 8; // 8 is the key size.

        Record[] records = new Record[numberOfRecords];
        for (int i = 0; i < numberOfRecords; i++) {
            byte[] key = Longs.toByteArray(i);
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
            byte[] key = Longs.toByteArray(i);
            byte[] value = TestUtils.generateRandomByteArray(valueSize);

            byte[] updatedValue = null;
            for (long j = 0; j < recordsPerFile; j++) {
                updatedValue = TestUtils.concatenateArrays(value, Longs.toByteArray(i));
                db.put(key, updatedValue);
            }

            // only store the last updated valued.
            records[i] = new Record(key, updatedValue);
        }

        return records;
    }
}
