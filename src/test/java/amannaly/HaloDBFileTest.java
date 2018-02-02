package amannaly;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class HaloDBFileTest extends TestBase {

    private File directory = Paths.get("tmp", "HaloDBFileTest",  "testIndexFile").toFile();
    private HaloDBFile file;
    private IndexFile indexFile;
    private int fileId = 100;

    @BeforeMethod
    public void before() throws IOException {
        TestUtils.deleteDirectory(directory);
        FileUtils.createDirectoryIfNotExists(directory);
        file = HaloDBFile.create(directory, fileId, new HaloDBOptions(), HaloDBFile.FileType.DATA_FILE);
        indexFile = new IndexFile(fileId, directory, new HaloDBOptions());
    }

    @AfterMethod
    public void after() throws IOException {
        if (file != null)
            file.close();
        if (indexFile != null)
            indexFile.close();
        TestUtils.deleteDirectory(directory);
    }

    @Test
    public void testIndexFile() throws IOException {
        List<Record> list = TestUtils.generateRandomData(1000);
        List<RecordMetaDataForCache> metaDataList = new ArrayList<>();
        for (Record record : list) {
            RecordMetaDataForCache r = file.writeRecord(record);
            metaDataList.add(r);
        }

        indexFile.open();
        IndexFile.IndexFileIterator iterator = indexFile.newIterator();

        int count = 0;
        while (iterator.hasNext()) {
            IndexFileEntry e = iterator.next();
            Record r = list.get(count);
            RecordMetaDataForCache meta = metaDataList.get(count);
            Assert.assertEquals(e.getKey(), r.getKey());

            int expectedOffset = meta.getValueOffset() - Record.Header.HEADER_SIZE - r.getKey().length;
            Assert.assertEquals(e.getRecordOffset(), expectedOffset);
            count++;
        }

        Assert.assertEquals(count, list.size());
    }

    @Test
    public void testRebuildIndexFile() throws IOException {
        List<Record> list = TestUtils.generateRandomData(1000);
        List<RecordMetaDataForCache> metaDataList = new ArrayList<>();
        for (Record record : list) {
            RecordMetaDataForCache r = file.writeRecord(record);
            metaDataList.add(r);
        }

        indexFile.delete();

        // make sure that the file is deleted. 
        Assert.assertFalse(Paths.get(directory.getName(), fileId + IndexFile.INDEX_FILE_NAME).toFile().exists());

        file.rebuildIndexFile();
        indexFile.open();
        IndexFile.IndexFileIterator iterator = indexFile.newIterator();

        int count = 0;
        while (iterator.hasNext()) {
            IndexFileEntry e = iterator.next();
            Record r = list.get(count);
            RecordMetaDataForCache meta = metaDataList.get(count);
            Assert.assertEquals(e.getKey(), r.getKey());

            int expectedOffset = meta.getValueOffset() - Record.Header.HEADER_SIZE - r.getKey().length;
            Assert.assertEquals(e.getRecordOffset(), expectedOffset);
            count++;
        }

        Assert.assertEquals(count, list.size());
    }
}
