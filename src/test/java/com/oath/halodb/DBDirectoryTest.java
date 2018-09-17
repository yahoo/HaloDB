/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Arjun Mannaly
 */
public class DBDirectoryTest {

    private static final File directory = Paths.get("tmp", "DBDirectoryTest").toFile();
    private DBDirectory dbDirectory;

    private static Integer[] dataFileIds = {7, 12, 1, 8, 10};
    private static Integer[] tombstoneFileIds = {21, 13, 12};

    @Test
    public void testListIndexFiles() {
        List<Integer> actual = dbDirectory.listIndexFiles();
        List<Integer> expected = Stream.of(dataFileIds).sorted().collect(Collectors.toList());
        Assert.assertEquals(actual, expected);
        Assert.assertEquals(actual.size(), dataFileIds.length);
    }

    @Test
    public void testListDataFiles() {
        File[] files = dbDirectory.listDataFiles();
        List<String> actual = Stream.of(files).map(File::getName).collect(Collectors.toList());
        List<String> expected = Stream.of(dataFileIds).map(i -> i + HaloDBFile.DATA_FILE_NAME).collect(Collectors.toList());
        MatcherAssert.assertThat(actual, Matchers.containsInAnyOrder(expected.toArray()));
        Assert.assertEquals(actual.size(), dataFileIds.length);
    }

    @Test
    public void testListTombstoneFiles() {
        File[] files = dbDirectory.listTombstoneFiles();
        List<String> actual = Stream.of(files).map(File::getName).collect(Collectors.toList());
        List<String> expected = Stream.of(tombstoneFileIds).sorted().map(i -> i + TombstoneFile.TOMBSTONE_FILE_NAME).collect(Collectors.toList());

        Assert.assertEquals(actual, expected);
        Assert.assertEquals(actual.size(), tombstoneFileIds.length);
    }

    @Test
    public void testSyncMetaDataNoError() {
        dbDirectory.syncMetaData();
    }

    @BeforeMethod
    public void createDirectory() throws IOException {
        dbDirectory = DBDirectory.open(directory);

        Path directoryPath = dbDirectory.getPath();
        for (int i : dataFileIds) {
            try(PrintWriter writer = new PrintWriter(new FileWriter(
                directoryPath.resolve(i + IndexFile.INDEX_FILE_NAME).toString()))) {
                writer.append("test");
            }

            try(PrintWriter writer = new PrintWriter(new FileWriter(
                directoryPath.resolve(i + HaloDBFile.DATA_FILE_NAME).toString()))) {
                writer.append("test");
            }
        }

        // repair file, should be skipped. 
        try(PrintWriter writer = new PrintWriter(new FileWriter(
            directoryPath.resolve(10000 + HaloDBFile.DATA_FILE_NAME + ".repair").toString()))) {
            writer.append("test");
        }

        for (int i : tombstoneFileIds) {
            try(PrintWriter writer = new PrintWriter(new FileWriter(
                directoryPath.resolve(i + TombstoneFile.TOMBSTONE_FILE_NAME).toString()))) {
                writer.append("test");
            }
        }

        // repair file, should be skipped.
        try(PrintWriter writer = new PrintWriter(new FileWriter(
            directoryPath.resolve(20000 + TombstoneFile.TOMBSTONE_FILE_NAME + ".repair").toString()))) {
            writer.append("test");
        }
    }

    @AfterMethod
    public void deleteDirectory() throws IOException {
        dbDirectory.close();
        TestUtils.deleteDirectory(directory);
    }
}
