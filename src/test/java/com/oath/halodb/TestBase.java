/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;

import java.io.File;
import java.io.IOException;

/**
 * @author Arjun Mannaly
 */
public class TestBase {

    private String directory;

    private HaloDB db;

    @DataProvider(name = "Options")
    public Object[][] optionData() {
        HaloDBOptions options = new HaloDBOptions();
        HaloDBOptions withMemoryPool = new HaloDBOptions();
        withMemoryPool.setUseMemoryPool(true);
        withMemoryPool.setMemoryPoolChunkSize(1024 * 1024);

        return new Object[][] {
            {options},
            {withMemoryPool}
        };
    }

    HaloDB getTestDB(String directory, HaloDBOptions options) throws HaloDBException {
        this.directory = directory;
        File dir = new File(directory);
        try {
            TestUtils.deleteDirectory(dir);
        } catch (IOException e) {
            throw new HaloDBException(e);
        }
        db = HaloDB.open(dir, options);
        return db;
    }

    HaloDB getTestDBWithoutDeletingFiles(String directory, HaloDBOptions options) throws HaloDBException {
        this.directory = directory;
        File dir = new File(directory);
        db = HaloDB.open(dir, options);
        return db;
    }

    @AfterMethod(alwaysRun = true)
    public void closeDB() throws HaloDBException, IOException {
        if (db != null) {
            db.close();
            File dir = new File(directory);
            TestUtils.deleteDirectory(dir);
        }
    }
}
