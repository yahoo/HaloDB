package amannaly;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class TestUtils {

    public static List<Record> insertRandomRecords(HaloDB db, int noOfRecords) throws IOException {
        List<Record> records = new ArrayList<>();
        Set<ByteBuffer> keySet = new HashSet<>();

        for (int i = 0; i < noOfRecords; i++) {
            byte[] key = TestUtils.generateRandomByteArray();
            while (keySet.contains(ByteBuffer.wrap(key))) {
                key = TestUtils.generateRandomByteArray();
            }
            ByteBuffer buf = ByteBuffer.wrap(key);
            keySet.add(buf);

            byte[] value = TestUtils.generateRandomByteArray();
            records.add(new Record(key, value));

            db.put(key, value);
        }

        return records;
    }

    public static List<Record> updateRecords(HaloDB db, List<Record> records) {
        List<Record> updated = new ArrayList<>();

        records.forEach(record -> {
            try {
                byte[] value = TestUtils.generateRandomByteArray();
                db.put(record.getKey(), value);
                updated.add(new Record(record.getKey(), value));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        return updated;
    }

    public static void deleteRecords(HaloDB db, List<Record> records) {
        records.forEach(r -> {
            try {
                db.delete(r.getKey());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static void deleteDirectory(File directory) throws IOException {
        if (!directory.exists())
            return;

        Path path = Paths.get(directory.getPath());
        SimpleFileVisitor<Path> visitor = new SimpleFileVisitor<Path>() {

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        };

        Files.walkFileTree(path, visitor);
    }

    public static byte[] concatenateArrays(byte[] a, byte[] b) {
        byte[] c = new byte[a.length + b.length];
        System.arraycopy(a, 0, c, 0, a.length);
        System.arraycopy(b, 0, c, a.length, b.length);

        return c;
    }

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

    public static byte[] generateRandomByteArray(int length) {
        byte[] array = new byte[length];
        random.nextBytes(array);

        return array;
    }

    public static byte[] generateRandomByteArray() {
        int length = random.nextInt(20) + 1;
        byte[] array = new byte[length];
        random.nextBytes(array);

        return array;
    }

    public static void waitForMergeToComplete(HaloDB db) throws InterruptedException {
        while (!db.isMergeComplete()) {
            Thread.sleep(1_000);
        }
    }
}
