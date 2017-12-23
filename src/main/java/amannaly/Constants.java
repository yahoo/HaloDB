package amannaly;

import java.util.regex.Pattern;

/**
 * @author Arjun Mannaly
 */
public class Constants {

    static final Pattern DATA_FILE_PATTERN = Pattern.compile("([0-9]+).data");

    static final Pattern INDEX_FILE_PATTERN = Pattern.compile("([0-9]+).index");

    static final Pattern TOMBSTONE_FILE_PATTERN = Pattern.compile("([0-9]+).tombstone");
}
