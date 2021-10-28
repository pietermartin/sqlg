package org.umlg.sqlg.test.tp3.structure;

import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.tp3.SqlgH2Provider;

import java.io.File;
import java.io.IOException;


/**
 * Executes the Standard Gremlin Structure Test Suite using H2.
 */
@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = SqlgH2Provider.class, graph = SqlgGraph.class)
public class SqlgH2StructureStandardTest {

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
