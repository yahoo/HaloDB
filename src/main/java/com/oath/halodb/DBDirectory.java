/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * @author Arjun Mannaly
 */
class DBDirectory {
    private static final Logger logger = LoggerFactory.getLogger(DBDirectory.class);

    private final File dbDirectory;
    private final FileChannel directoryChannel;

    private DBDirectory(File dbDirectory, FileChannel directoryChannel) {
        this.dbDirectory = dbDirectory;
        this.directoryChannel = directoryChannel;
    }

    /**
     * Will create a new directory if one doesn't already exist.
     */
    static DBDirectory open(File directory) throws IOException {
        FileUtils.createDirectoryIfNotExists(directory);
        return new DBDirectory(directory, openReadOnlyChannel(directory));
    }

    void close() throws IOException {
        directoryChannel.close();
    }

    Path getPath() {
        return dbDirectory.toPath();
    }

    File getDirectory() {
        return dbDirectory;
    }

    File[] listDataFiles() {
        return FileUtils.listDataFiles(dbDirectory);
    }

    List<Integer> listIndexFiles() {
        return FileUtils.listIndexFiles(dbDirectory);
    }

    File[] listTombstoneFiles() {
        return FileUtils.listTombstoneFiles(dbDirectory);
    }

    /**
     * calling fsync on a directory works on Java 8 running on Linux and OSX.
     * This may not work on other platforms. Therefore in case there is an exception
     * we silently swallow it.
     */
    void syncMetaData() {
        try {
            directoryChannel.force(true);
        } catch (IOException e) {
        }
    }

    private static FileChannel openReadOnlyChannel(File dbDirectory) throws IOException {
        return  FileChannel.open(dbDirectory.toPath(), StandardOpenOption.READ);
    }
}
