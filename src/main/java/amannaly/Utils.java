package amannaly;

/**
 * @author Arjun Mannaly
 */
class Utils {
    static long roundUpToPowerOf2(long number) {
        return (number > 1) ? Long.highestOneBit((number - 1) << 1) : 1;
    }

    static int getValueOffset(int recordOffset, byte[] key) {
        return recordOffset + Record.Header.HEADER_SIZE + key.length;
    }
}
