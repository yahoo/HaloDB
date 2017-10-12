package amannaly;

import com.google.protobuf.ByteString;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

        int sizeOfHintEntry = HintFileEntry.HINT_FILE_HEADER_SIZE + 8;
        Map<Long, List<Path>> hintFileSizemap = Files.list(directory.toPath())
            .filter(path -> HaloDBInternal.HINT_FILE_PATTERN.matcher(path.getFileName().toString()).matches())
            .collect(Collectors.groupingBy(path -> path.toFile().length()));

        // 4 hint files of size 220 bytes.
        Assert.assertEquals(hintFileSizemap.get(sizeOfHintEntry * 10l).size(), 4);

        // 2 hint files of size 440 bytes.
        Assert.assertEquals(hintFileSizemap.get(sizeOfHintEntry * 20l).size(), 2);

        for (Record r : records) {
            ByteString actual = db.get(r.getKey());
            Assert.assertEquals(actual, r.getValue());
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
            ByteString actual = db.get(r.getKey());
            Assert.assertEquals(actual, r.getValue());
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
            ByteString actual = db.get(r.getKey());
            Assert.assertEquals(actual, r.getValue());
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
            ByteString actual = db.get(r.getKey());
            Assert.assertEquals(actual, r.getValue());
        }

        db.close();

        TestUtils.deleteDirectory(directory);
    }

    private Record[] insertAndUpdateRecords(int numberOfRecords, HaloDB db) throws IOException {
        byte[] data = new byte[recordSize - Record.HEADER_SIZE - 8 - 8];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte)i;
        }

        Record[] records = new Record[numberOfRecords];
        for (int i = 0; i < numberOfRecords; i++) {
            ByteString key = ByteString.copyFrom(Utils.longToBytes(i));
            ByteString value = ByteString.copyFrom(data).concat(key);
            records[i] = new Record(key, value);
            db.put(records[i].getKey(), records[i].getValue());
        }

        // modify first 5 records of each file.
        ByteString modifiedMark = ByteString.copyFromUtf8("modified");
        for (int k = 0; k < numberOfFiles; k++) {
            for (int i = 0; i < 5; i++) {
                Record r = records[i + k*10];
                ByteString modifiedValue = r.getValue().substring(modifiedMark.size()).concat(modifiedMark);
                Record modifiedRecord = new Record(r.getKey(), modifiedValue);
                records[i + k*10] = modifiedRecord;
                db.put(modifiedRecord.getKey(), modifiedRecord.getValue());
            }
        }
        return records;
    }

    private Record[] insertAndUpdateRecordsToSameFile(int numberOfRecords, HaloDB db) throws IOException {
        byte[] data = new byte[recordSize - Record.HEADER_SIZE - 8 - 8 - 8];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte)i;
        }

        Record[] records = new Record[numberOfRecords];
        for (int i = 0; i < numberOfRecords; i++) {
            ByteString key = ByteString.copyFrom(Utils.longToBytes(i));
            ByteString value = ByteString.copyFrom(data).concat(key);

            ByteString updatedValue = null;
            for (long j = 0; j < recordsPerFile; j++) {
                updatedValue = value.concat(ByteString.copyFrom(Utils.longToBytes(j)));
                db.put(key, updatedValue);
            }

            // only store the last updated valued.
            records[i] = new Record(key, updatedValue);
        }

        return records;
    }
}
