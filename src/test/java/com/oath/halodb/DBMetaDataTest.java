/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author Arjun Mannaly
 */
public class DBMetaDataTest {

    private static final String directory = Paths.get("tmp", "DBMetaDataTest",  "testDBMetaData").toString();

    @Test
    public void testDBMetaData() throws IOException {
        Path metaDataFile = Paths.get(directory, DBMetaData.METADATA_FILE_NAME);

        // confirm that the file doesn't exist.
        Assert.assertFalse(Files.exists(metaDataFile));

        DBMetaData metaData = new DBMetaData(directory);
        metaData.loadFromFileIfExists();

        // file has not yet been created, return default values.
        Assert.assertEquals(metaData.getVersion(), 0);
        Assert.assertEquals(metaData.getMaxFileSize(), 0);
        Assert.assertFalse(metaData.isOpen());
        Assert.assertEquals(metaData.getSequenceNumber(), 0);
        Assert.assertFalse(metaData.isIOError());

        metaData.setVersion(Versions.CURRENT_META_FILE_VERSION);
        metaData.setOpen(true);
        metaData.setSequenceNumber(100);
        metaData.setIOError(false);
        metaData.setMaxFileSize(100);
        metaData.storeToFile();

        // confirm that the file has been created.
        Assert.assertTrue(Files.exists(metaDataFile));

        // load again to read stored values.
        metaData = new DBMetaData(directory);
        metaData.loadFromFileIfExists();

        Assert.assertEquals(metaData.getVersion(), Versions.CURRENT_META_FILE_VERSION);
        Assert.assertTrue(metaData.isOpen());
        Assert.assertEquals(metaData.getSequenceNumber(), 100);
        Assert.assertFalse(metaData.isIOError());
        Assert.assertEquals(metaData.getMaxFileSize(), 100);

        metaData.setVersion(Versions.CURRENT_META_FILE_VERSION + 10);
        metaData.setOpen(false);
        metaData.setSequenceNumber(Long.MAX_VALUE);
        metaData.setIOError(true);
        metaData.setMaxFileSize(1024);
        metaData.storeToFile();

        // load again to read stored values.
        metaData = new DBMetaData(directory);
        metaData.loadFromFileIfExists();

        Assert.assertEquals(metaData.getVersion(), Versions.CURRENT_META_FILE_VERSION + 10);
        Assert.assertFalse(metaData.isOpen());
        Assert.assertEquals(metaData.getSequenceNumber(), Long.MAX_VALUE);
        Assert.assertTrue(metaData.isIOError());
        Assert.assertEquals(metaData.getMaxFileSize(), 1024);
    }

    @Test
    public void testCheckSum() throws IOException {
        DBMetaData metaData = new DBMetaData(directory);
        metaData.loadFromFileIfExists();

        metaData.setVersion(Versions.CURRENT_META_FILE_VERSION);
        metaData.setOpen(true);
        metaData.setSequenceNumber(100);
        metaData.setIOError(false);
        metaData.setMaxFileSize(100);
        metaData.storeToFile();

        metaData = new DBMetaData(directory);
        metaData.loadFromFileIfExists();

        Assert.assertTrue(metaData.isValid());

    }

    @BeforeMethod
    public void createDirectory() throws IOException {
        FileUtils.createDirectoryIfNotExists(new File(directory));
    }

    @AfterMethod
    public void deleteDirectory() throws IOException {
        TestUtils.deleteDirectory(new File(directory));
    }

}
