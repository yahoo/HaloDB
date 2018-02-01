package amannaly;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Arjun Mannaly
 */
class FileUtils {

    static void createDirectoryIfNotExists(File directory) throws IOException {
        if (directory.exists() && directory.isDirectory())
            return;

        if (!directory.mkdirs()) {
            throw new IOException("Cannot create directory " + directory.getName());
        }
    }

    static List<Integer> listIndexFiles(File directory) {
        File[] files = directory.listFiles(file -> Constants.INDEX_FILE_PATTERN.matcher(file.getName()).matches());

        // sort in ascending order. we want the earliest index files to be processed first.
        return
            Arrays.stream(files)
                // extract file id. 
                .map(file -> Constants.INDEX_FILE_PATTERN.matcher(file.getName()))
                .map(matcher -> {
                    matcher.find();
                    return matcher.group(1);
                })
                .map(Integer::valueOf)
                .sorted()
                .collect(Collectors.toList());
    }

    static File[] listTombstoneFiles(File directory) {
        return directory.listFiles(file -> Constants.TOMBSTONE_FILE_PATTERN.matcher(file.getName()).matches());
    }

    static File[] listDataFiles(File directory) {
        return directory.listFiles(file -> Constants.DATA_FILE_PATTERN.matcher(file.getName()).matches());
    }
}
