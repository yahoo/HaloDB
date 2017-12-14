package amannaly;

/**
 * @author Arjun Mannaly
 */
public class Utils {

    public static byte[] longToBytes(long value) {
        byte[] bytes = new byte[8];

        for(int i = 0; i < bytes.length; ++i) {
            bytes[bytes.length - 1 - i] = (byte)((int)value);
            value >>>= 8;
        }

        return bytes;
    }

    public static long bytesToLong(byte[] value) {

        long longValue = 0L;

        for (int i = 0; i < value.length; i++) {
            // Left shift has no effect thru first iteration of loop.
            longValue <<= 8;
            longValue ^= value[i] & 0xFF;
        }

        return longValue;
    }

    public static long roundUpToPowerOf2(long number) {
        return (number > 1) ? Long.highestOneBit((number - 1) << 1) : 1;
    }
}
