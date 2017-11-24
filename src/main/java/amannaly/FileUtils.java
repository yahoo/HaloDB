package amannaly;

import java.io.File;
import java.io.IOException;

/**
 * @author Arjun Mannaly
 */
class FileUtils {

    static void createDirectory(File directory) throws IOException {
        if (directory.exists()  && directory.isDirectory())
            return;

        if(!directory.mkdirs()) {
            throw new IOException("Cannot create directory " + directory.getName());
        }
    }

}
