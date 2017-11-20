package amannaly;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;
import java.io.IOException;

/**
 * @author Arjun Mannaly
 */
public class HaloTestDB implements TestRule {

    private String directory;

    private HaloDB db;

    public HaloDB getTestDB(String directory, HaloDBOptions options) throws IOException {
        this.directory = directory;
        File dir = new File(directory);
        TestUtils.deleteDirectory(dir);
        db = HaloDB.open(dir, options);
        return db;
    }

    public HaloDB getTestDBWithoutDeletingFiles(String directory, HaloDBOptions options) throws IOException {
        this.directory = directory;
        File dir = new File(directory);
        db = HaloDB.open(dir, options);
        return db;
    }

    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    base.evaluate();
                }
                finally {
                    if (db != null) {
                        db.close();
                        File dir = new File(directory);
                        TestUtils.deleteDirectory(dir);
                    }
                }
            }
        };
    }
}
