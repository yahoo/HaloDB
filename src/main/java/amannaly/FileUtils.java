package amannaly;

import java.io.File;
import java.io.IOException;

public class FileUtils {

    public static void createDirectory(File directory) throws IOException {
        if (directory.exists()  && directory.isDirectory())
            return;

        if(!directory.mkdirs()) {
            throw new IOException("Cannot create directory " + directory.getName());
        }
    }

}
