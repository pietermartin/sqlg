package org.umlg.sqlg.test;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.umlg.sqlg.AllTest;

import java.io.File;
import java.io.IOException;

/**
 * @author Lukas Krejci
 * @since 1.3.0
 */
public class H2AllTest extends AllTest {

    @BeforeClass
    public static void setUp() {
        try {
            FileUtils.cleanDirectory(new File("./src/test/db/"));
        } catch (IOException e) {
            Assert.fail("Failed to delete H2's db dir at ./src/test/db");
        }
    }
}
