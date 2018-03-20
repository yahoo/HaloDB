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
public class HaloDBCompactionTest extends TestBase {

    private final int recordSize = 1024;
    private final int numberOfFiles = 8;
    private final int recordsPerFile = 10;
    private final int numberOfRecords = numberOfFiles * recordsPerFile;

    @Test
    public void testMerge() throws Exception {
        String directory = "/tmp/HaloDBCompactionTest/testMerge";

        HaloDBOptions options = new HaloDBOptions();
        options.maxFileSize = recordsPerFile * recordSize;
        options.mergeThresholdPerFile = 0.5;
        options.isCompactionDisabled = false;
        options.flushDataSizeBytes = 2048;

        HaloDB db =  getTestDB(directory, options);

        Record[] records = insertAndUpdateRecords(numberOfRecords, db);

        TestUtils.waitForCompactionToComplete(db);

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
    public void testReOpenDBAfterMerge() throws IOException {
        String directory = "/tmp/HaloDBCompactionTest/testReOpenDBAfterMerge";

        HaloDBOptions options = new HaloDBOptions();
        options.maxFileSize = recordsPerFile * recordSize;
        options.mergeThresholdPerFile = 0.5;
        options.isCompactionDisabled = false;

        HaloDB db = getTestDB(directory, options);

        Record[] records = insertAndUpdateRecords(numberOfRecords, db);

        TestUtils.waitForCompactionToComplete(db);

        db.close();

        db = getTestDBWithoutDeletingFiles(directory, options);

        for (Record r : records) {
            byte[] actual = db.get(r.getKey());
            Assert.assertEquals(actual, r.getValue());
        }
    }

    @Test
    public void testReOpenDBWithoutMerge() throws IOException {
        String directory ="/tmp/HaloDBCompactionTest/testReOpenAndUpdatesAndWithoutMerge";

        HaloDBOptions options = new HaloDBOptions();
        options.maxFileSize = recordsPerFile * recordSize;
        options.isCompactionDisabled = true;

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
    public void testUpdatesToSameFile() throws IOException {
        String directory ="/tmp/HaloDBCompactionTest/testUpdatesToSameFile";

        HaloDBOptions options = new HaloDBOptions();
        options.maxFileSize = recordsPerFile * recordSize;
        options.isCompactionDisabled = true;

        HaloDB db = getTestDB(directory, options);

        Record[] records = insertAndUpdateRecordsToSameFile(2, db);

        db.close();

        db = getTestDBWithoutDeletingFiles(directory, options);

        for (Record r : records) {
            byte[] actual = db.get(r.getKey());
            Assert.assertEquals(actual, r.getValue());
        }
    }

    @Test
    public void testFilesWithStaleDataAddedToCompactionQueueDuringDBOpen() throws IOException, InterruptedException {
        String directory = Paths.get("tmp", "HaloDBCompactionTest", "testFilesWithStaleDataAddedToCompactionQueueDuringDBOpen").toString();

        HaloDBOptions options = new HaloDBOptions();
        options.isCompactionDisabled = true;
        options.maxFileSize = 10 * 1024;

        HaloDB db = getTestDB(directory, options);

        // insert 50 records into 5 files.
        List<Record> records = TestUtils.insertRandomRecordsOfSize(db, 50, 1024-Record.Header.HEADER_SIZE);

        // Delete all records, which means that all data files would have crossed the
        // stale data threshold.  
        for (Record r : records) {
            db.delete(r.getKey());
        }

        db.close();

        // open the db withe compaction enabled. 
        options = new HaloDBOptions();
        options.maxFileSize = 10 * 1024;

        db = getTestDBWithoutDeletingFiles(directory, options);

        TestUtils.waitForCompactionToComplete(db);

        // Since all files have crossed stale data threshold, everything will be compacted and deleted.
        Assert.assertFalse(TestUtils.getLatestDataFile(directory).isPresent());
        Assert.assertFalse(TestUtils.getLatestCompactionFile(directory).isPresent());

        db.close();

        // open the db with compaction disabled.
        options = new HaloDBOptions();
        options.maxFileSize = 10 * 1024;
        options.isCompactionDisabled = true;

        db = getTestDBWithoutDeletingFiles(directory, options);

        // insert 20 records into two files. 
        records = TestUtils.insertRandomRecordsOfSize(db, 20, 1024-Record.Header.HEADER_SIZE);
        File[] dataFilesToDelete = FileUtils.listDataFiles(new File(directory));

        // update all records; since compaction is disabled no file is deleted.
        List<Record> updatedRecords = TestUtils.updateRecords(db, records);

        db.close();

        // Open db again with compaction enabled. 
        options = new HaloDBOptions();
        options.maxFileSize = 10 * 1024;

        db = getTestDBWithoutDeletingFiles(directory, options);
        TestUtils.waitForCompactionToComplete(db);

        //Confirm that previous data files were compacted and deleted.
        for (File f : dataFilesToDelete) {
            Assert.assertFalse(f.exists());
        }

        for (Record r : updatedRecords) {
            Assert.assertEquals(db.get(r.getKey()), r.getValue());
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
