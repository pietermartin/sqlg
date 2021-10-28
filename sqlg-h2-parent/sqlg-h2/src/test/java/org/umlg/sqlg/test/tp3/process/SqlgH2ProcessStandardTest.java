package org.umlg.sqlg.test.tp3.process;

import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.tp3.SqlgH2Provider;

import java.io.File;
import java.io.IOException;


/**
 * Executes the Standard Gremlin Structure Test Suite using Hsqldb.
 */
@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = SqlgH2Provider.class, graph = SqlgGraph.class)
public class SqlgH2ProcessStandardTest {

    @BeforeClass
    public static void setUp() {
        try {
            //noinspection ResultOfMethodCallIgnored
            new File("./src/test/db/").mkdirs();
            FileUtils.cleanDirectory(new File("./src/test/db/"));
        } catch (IOException e) {
            Assert.fail("Failed to delete H2's db dir at ./src/test/db");
        }
    }
}
