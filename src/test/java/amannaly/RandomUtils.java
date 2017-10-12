package amannaly;

import java.util.Random;

public class RandomUtils {

    private static Random random = new Random();

    public static String generateRandomAsciiString(int length) {
        StringBuilder builder = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            int next = 48 + random.nextInt(78);
            builder.append((char)next);
        }

        return builder.toString();
    }

    public static String generateRandomAsciiString() {
        int length = random.nextInt(20) + 1;
        StringBuilder builder = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            int next = 48 + random.nextInt(78);
            builder.append((char)next);
        }

        return builder.toString();
    }

}
