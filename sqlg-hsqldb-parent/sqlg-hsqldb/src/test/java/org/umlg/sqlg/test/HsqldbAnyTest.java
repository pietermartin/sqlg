package org.umlg.sqlg.test;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.umlg.sqlg.AnyTest;

import java.io.File;
import java.io.IOException;

/**
 * Date: 2014/07/16
 * Time: 12:11 PM
 */
public class HsqldbAnyTest extends AnyTest {

    @BeforeClass
    public static void setUp() {
        try {
            FileUtils.cleanDirectory(new File("./src/test/db/"));
        } catch (IOException e) {
            Assert.fail("Failed to delete Hsqldb's db dir at ./src/test/db");
        }
    }
}
