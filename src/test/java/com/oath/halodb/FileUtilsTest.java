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
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileUtilsTest {

    private String directory = TestUtils.getTestDirectory("FileUtilsTest");

    private Integer[] fileIds = {7, 12, 1, 8, 10};

    private List<String> indexFileNames =
        Stream.of(fileIds)
            .map(i -> Paths.get(directory).resolve(i + IndexFile.INDEX_FILE_NAME).toString())
            .collect(Collectors.toList());


    private List<String> dataFileNames =
        Stream.of(fileIds)
            .map(i -> Paths.get(directory).resolve(i + HaloDBFile.DATA_FILE_NAME).toString())
            .collect(Collectors.toList());


    private List<String> dataFileNamesRepair =
        Stream.of(fileIds)
            .map(i -> Paths.get(directory).resolve(i + HaloDBFile.DATA_FILE_NAME + ".repair").toString())
            .collect(Collectors.toList());


    private List<String> tombstoneFileNames =
        Stream.of(fileIds)
            .map(i -> Paths.get(directory).resolve(i + TombstoneFile.TOMBSTONE_FILE_NAME).toString())
            .collect(Collectors.toList());


    @BeforeMethod
    public void createDirectory() throws IOException {
        TestUtils.deleteDirectory(new File(directory));
        FileUtils.createDirectoryIfNotExists(new File(directory));

        for (String f : indexFileNames) {
            try(PrintWriter writer = new PrintWriter(new FileWriter(f))) {
                writer.append("test");
            }
        }

        for (String f : dataFileNames) {
            try(PrintWriter writer = new PrintWriter(new FileWriter(f))) {
                writer.append("test");
            }
        }

        for (String f : dataFileNamesRepair) {
            try(PrintWriter writer = new PrintWriter(new FileWriter(f))) {
                writer.append("test");
            }
        }

        for (String f : tombstoneFileNames) {
            try(PrintWriter writer = new PrintWriter(new FileWriter(f))) {
                writer.append("test");
            }
        }
    }

    @AfterMethod
    public void deleteDirectory() throws IOException {
        TestUtils.deleteDirectory(new File(directory));
    }

    @Test
    public void testListIndexFiles() {
        List<Integer> actual = FileUtils.listIndexFiles(new File(directory));

        List<Integer> expected = Stream.of(fileIds).sorted().collect(Collectors.toList());
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testListDataFiles() {
        File[] files = FileUtils.listDataFiles(new File(directory));
        List<String> actual = Stream.of(files).map(File::getName).collect(Collectors.toList());
        List<String> expected = Stream.of(fileIds).map(i -> i + HaloDBFile.DATA_FILE_NAME).collect(Collectors.toList());
        MatcherAssert.assertThat(actual, Matchers.containsInAnyOrder(expected.toArray()));
    }

    @Test
    public void testListTombstoneFiles() {
        File[] files = FileUtils.listTombstoneFiles(new File(directory));
        List<String> actual = Stream.of(files).map(File::getName).collect(Collectors.toList());
        List<String> expected = Stream.of(fileIds).sorted().map(i -> i + TombstoneFile.TOMBSTONE_FILE_NAME).collect(Collectors.toList());

        Assert.assertEquals(actual, expected);
    }

}
