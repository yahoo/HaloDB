package amannaly;

import org.junit.Rule;

/**
 * @author Arjun Mannaly
 */
public abstract class TestBase {

    @Rule
    public HaloTestDB testDB = new HaloTestDB();
}
