package amannaly;

import org.testng.annotations.AfterMethod;

import java.io.File;
import java.io.IOException;

/**
 * @author Arjun Mannaly
 */
public class TestBase {

    private String directory;

    private HaloDB db;

    HaloDB getTestDB(String directory, HaloDBOptions options) throws IOException {
        this.directory = directory;
        File dir = new File(directory);
        TestUtils.deleteDirectory(dir);
        db = HaloDB.open(dir, options);
        return db;
    }

    HaloDB getTestDBWithoutDeletingFiles(String directory, HaloDBOptions options) throws IOException {
        this.directory = directory;
        File dir = new File(directory);
        db = HaloDB.open(dir, options);
        return db;
    }

    @AfterMethod(alwaysRun = true)
    public void closeDB() throws IOException {
        if (db != null) {
            db.close();
            File dir = new File(directory);
            TestUtils.deleteDirectory(dir);
        }
    }
}
