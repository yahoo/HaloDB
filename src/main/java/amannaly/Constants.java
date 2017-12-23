package amannaly;

import java.util.regex.Pattern;

/**
 * @author Arjun Mannaly
 */
public class Constants {

    static final Pattern DATA_FILE_PATTERN = Pattern.compile("([0-9]+)" + HaloDBFile.DATA_FILE_NAME);

    static final Pattern INDEX_FILE_PATTERN = Pattern.compile("([0-9]+)" + IndexFile.INDEX_FILE_NAME);

    static final Pattern TOMBSTONE_FILE_PATTERN = Pattern.compile("([0-9]+)" + TombstoneFile.TOMBSTONE_FILE_NAME);
}
