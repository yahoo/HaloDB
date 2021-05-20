package com.oath.halodb;

import org.testng.Assert;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

public class HaloDBOptionsTest  extends TestBase {

    @Test
    public void testDefaultOptions() throws HaloDBException {
        String directory = TestUtils.getTestDirectory("HaloDBOptionsTest", "testDefaultOptions");

        HaloDB db = getTestDB(directory, new HaloDBOptions());
        Assert.assertFalse(db.stats().getOptions().isSyncWrite());
        Assert.assertFalse(db.stats().getOptions().isCompactionDisabled());
        Assert.assertEquals(db.stats().getOptions().getBuildIndexThreads(), 1);
    }

    @Test
    public void testSetBuildIndexThreads() {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        HaloDBOptions options = new HaloDBOptions();

        // Test valid boundaries.
        if (availableProcessors > 1) {
            options.setBuildIndexThreads(availableProcessors);
            Assert.assertEquals(options.getBuildIndexThreads(), availableProcessors);
        }
        options.setBuildIndexThreads(1);
        Assert.assertEquals(options.getBuildIndexThreads(), 1);

        // Test invalid boundaries.
        assertThatIllegalArgumentException().isThrownBy(() -> options.setBuildIndexThreads(0));
        assertThatIllegalArgumentException().isThrownBy(() -> options.setBuildIndexThreads(availableProcessors + 1));
    }
}
