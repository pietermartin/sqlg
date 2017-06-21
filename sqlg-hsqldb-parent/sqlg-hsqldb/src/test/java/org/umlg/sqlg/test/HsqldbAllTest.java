package org.umlg.sqlg.test;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.umlg.sqlg.AllTest;

import java.io.File;
import java.io.IOException;

/**
 * Date: 2014/07/16
 * Time: 12:11 PM
 */
public class HsqldbAllTest extends AllTest {

    @BeforeClass
    public static void setUp() {
        try {
            System.out.println(new File(".").getAbsolutePath());
            FileUtils.cleanDirectory(new File("./src/test/db/"));
        } catch (IOException e) {
            Assert.fail("Failed to delete Hsqldb's db dir at ./src/test/db");
        }
    }

}
