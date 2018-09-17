/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * Represents the top level directory for a HaloDB instance. 
 *
 * @author Arjun Mannaly
 */
class DBDirectory {

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
     * In Linux the recommended way to flush directory metadata is to open a
     * file descriptor for the directory and to call fsync on it. In Java opening a read-only file channel
     * and calling force(true) will do the same for us. But this is an undocumented behavior
     * in Java and could change in future versions.
     * https://grokbase.com/t/lucene/dev/1519kz2s50/recent-java-9-commit-e5b66323ae45-breaks-fsync-on-directory
     *
     * This currently works on Linux and OSX but may not work on other platforms. Therefore, if there is
     * an exception we silently swallow it.
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
