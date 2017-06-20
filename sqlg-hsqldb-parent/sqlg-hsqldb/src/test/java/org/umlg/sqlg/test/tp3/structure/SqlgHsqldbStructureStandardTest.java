package org.umlg.sqlg.test.tp3.structure;

import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.tp3.SqlgHsqldbProvider;

import java.io.File;
import java.io.IOException;


/**
 * Executes the Standard Gremlin Structure Test Suite using Hsqldb.
 */
@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = SqlgHsqldbProvider.class, graph = SqlgGraph.class)
public class SqlgHsqldbStructureStandardTest {

    @BeforeClass
    public static void setUp() {
        try {
            FileUtils.cleanDirectory(new File("./src/test/db/"));
        } catch (IOException e) {
            Assert.fail("Failed to delete Hsqldb's db dir at ./src/test/db");
        }
    }

}
