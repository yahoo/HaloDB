package amannaly;

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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileUtilsTest {

    private String directory = Paths.get("tmp", "FileUtilsTest").toString();

    private Integer[] fileIds = {7, 12, 1, 8, 10};

    private List<String> indexFileNames =
        Stream.of(fileIds)
            .map(i -> Paths.get(directory).resolve(i + IndexFile.INDEX_FILE_NAME).toString())
            .collect(Collectors.toList());


    private List<String> dataFileNames =
        Stream.of(fileIds)
            .map(i -> Paths.get(directory).resolve(i + HaloDBFile.DATA_FILE_NAME).toString())
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

}
