/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

/**
 * @author Arjun Mannaly
 */
public class TestUtils {
    private static final Logger logger = LoggerFactory.getLogger(TestUtils.class);

    static String getTestDirectory(String... path) {
        return Paths.get("tmp", path).toString();
    }

    static List<Record> insertRandomRecords(HaloDB db, int noOfRecords) throws HaloDBException {
        return insertRandomRecordsOfSize(db, noOfRecords, -1);
    }

    static List<Record> insertRandomRecordsOfSize(HaloDB db, int noOfRecords, int size) throws HaloDBException {
        List<Record> records = new ArrayList<>();
        Set<ByteBuffer> keySet = new HashSet<>();
        Random random = new Random();

        for (int i = 0; i < noOfRecords; i++) {
            byte[] key;
            if (size > 0) {
             key = TestUtils.generateRandomByteArray(random.nextInt(Math.min(Byte.MAX_VALUE-1, size))+1);
            }
            else {
                key = TestUtils.generateRandomByteArray();
            }
            while (keySet.contains(ByteBuffer.wrap(key))) {
                key = TestUtils.generateRandomByteArray();
            }
            ByteBuffer buf = ByteBuffer.wrap(key);
            keySet.add(buf);

            byte[] value;
            if (size > 0) {
                value = TestUtils.generateRandomByteArray(size - key.length);
            }
            else {
                value = TestUtils.generateRandomByteArray();
            }
            records.add(new Record(key, value));

            db.put(key, value);
        }

        return records;
    }

    static List<Record> generateRandomData(int noOfRecords) {
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
        }

        return records;

    }

    static List<Record> updateRecords(HaloDB db, List<Record> records) {
        List<Record> updated = new ArrayList<>();

        records.forEach(record -> {
            try {
                byte[] value = TestUtils.generateRandomByteArray();
                db.put(record.getKey(), value);
                updated.add(new Record(record.getKey(), value));
            } catch (HaloDBException e) {
                throw new RuntimeException(e);
            }
        });

        return updated;
    }

    static List<Record> updateRecordsWithSize(HaloDB db, List<Record> records, int size) {
        List<Record> updated = new ArrayList<>();

        records.forEach(record -> {
            try {
                byte[] value = TestUtils.generateRandomByteArray(size-record.getKey().length-Record.Header.HEADER_SIZE);
                db.put(record.getKey(), value);
                updated.add(new Record(record.getKey(), value));
            } catch (HaloDBException e) {
                throw new RuntimeException(e);
            }
        });

        return updated;
    }

    static void deleteRecords(HaloDB db, List<Record> records) {
        records.forEach(r -> {
            try {
                db.delete(r.getKey());
            } catch (HaloDBException e) {
                throw new RuntimeException(e);
            }
        });
    }

    static void deleteDirectory(File directory) throws IOException {
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

    static byte[] concatenateArrays(byte[] a, byte[] b) {
        byte[] c = new byte[a.length + b.length];
        System.arraycopy(a, 0, c, 0, a.length);
        System.arraycopy(b, 0, c, a.length, b.length);

        return c;
    }

    private static Random random = new Random();

    static String generateRandomAsciiString(int length) {
        StringBuilder builder = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            int next = 48 + random.nextInt(78);
            builder.append((char)next);
        }

        return builder.toString();
    }

    static String generateRandomAsciiString() {
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
        int length = random.nextInt(Byte.MAX_VALUE) + 1;
        byte[] array = new byte[length];
        random.nextBytes(array);

        return array;
    }

    /**
     * This method will work correctly only after all the writes to the db have been completed.
     */
    static void waitForCompactionToComplete(HaloDB db) {
        while (!db.isCompactionComplete()) {
            try {
                Thread.sleep(1_000);
            } catch (InterruptedException e) {
                logger.error("Thread interrupted while waiting for compaction to complete");
                throw new RuntimeException(e);
            }
        }
    }

    static Optional<File> getLatestDataFile(String directory) {
        return Arrays.stream(FileUtils.listDataFiles(new File(directory)))
            .filter(f -> HaloDBFile.findFileType(f) == HaloDBFile.FileType.DATA_FILE)
            .max(Comparator.comparing(File::getName));
    }

    static Optional<File> getLatestCompactionFile(String directory) {
        return Arrays.stream(FileUtils.listDataFiles(new File(directory)))
            .filter(f -> HaloDBFile.findFileType(f) == HaloDBFile.FileType.COMPACTED_FILE)
            .max(Comparator.comparing(File::getName));
    }

    static FileTime getFileCreationTime(File file) throws IOException {
        return Files.readAttributes(file.toPath(), BasicFileAttributes.class).creationTime();
    }
}
