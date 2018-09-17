/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import com.google.common.primitives.Longs;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * @author Arjun Mannaly
 */
public class HaloDBFileCompactionTest extends TestBase {

    @Test(dataProvider = "Options")
    public void testCompaction(HaloDBOptions options) throws Exception {
        String directory = TestUtils.getTestDirectory("HaloDBFileCompactionTest", "testCompaction");

        int recordSize = 1024;
        int recordNumber = 20;

        options.setMaxFileSize(10 * recordSize); // 10 records per data file.
        options.setCompactionThresholdPerFile(0.5);

        HaloDB db = getTestDB(directory, options);

        byte[] data = new byte[recordSize - Record.Header.HEADER_SIZE - 8 - 8];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte)i;
        }

        Record[] records = new Record[recordNumber];
        for (int i = 0; i < recordNumber; i++) {
            byte[] key = Longs.toByteArray(i);
            byte[] value = TestUtils.concatenateArrays(data, key);
            records[i] = new Record(key, value);
            db.put(records[i].getKey(), records[i].getValue());
        }

        List<Record> freshRecords = new ArrayList<>();

        // There are two data files. make the first half of both the files stale. 
        for (int i = 0; i < 5; i++) {
            db.put(records[i].getKey(), records[i].getValue());
            db.put(records[i+10].getKey(), records[i+10].getValue());
        }

        // Second half of both the files should be copied to the compacted file.
        for (int i = 5; i < 10; i++) {
            freshRecords.add(records[i]);
            freshRecords.add(records[i + 10]);
        }

        TestUtils.waitForCompactionToComplete(db);

        // the latest file will be the compacted file.
        File compactedFile = Arrays.stream(FileUtils.listDataFiles(new File(directory))).max(Comparator.comparing(File::getName)).get();
        HaloDBFile.HaloDBFileIterator iterator = HaloDBFile.openForReading(dbDirectory, compactedFile, HaloDBFile.FileType.COMPACTED_FILE, options).newIterator();

        // make sure the the compacted file has the bottom half of two files.
        List<Record> mergedRecords = new ArrayList<>();
        while (iterator.hasNext()) {
            mergedRecords.add(iterator.next());
        }

        MatcherAssert.assertThat(freshRecords, Matchers.containsInAnyOrder(mergedRecords.toArray()));
    }
}
