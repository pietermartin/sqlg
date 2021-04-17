package org.umlg.sqlg.test;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.umlg.sqlg.AnyTest;

import java.io.File;
import java.io.IOException;

/**
 * @author Lukas Krejci
 * @since 1.3.0
 */
public class H2AnyTest extends AnyTest {

    @BeforeClass
    public static void setUp() {
        try {
            File directory = new File("./src/test/db/");
            if (directory.exists()) {
                FileUtils.cleanDirectory(directory);
            } else {
                //noinspection ResultOfMethodCallIgnored
                directory.mkdir();
            }
        } catch (IOException e) {
            Assert.fail("Failed to delete H2's db dir at ./src/test/db");
        }
    }
}
