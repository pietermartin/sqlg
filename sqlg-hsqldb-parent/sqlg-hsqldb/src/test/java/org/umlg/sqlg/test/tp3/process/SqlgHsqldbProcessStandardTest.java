package org.umlg.sqlg.test.tp3.process;

import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
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
@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = SqlgHsqldbProvider.class, graph = SqlgGraph.class)
public class SqlgHsqldbProcessStandardTest {

    @BeforeClass
    public static void setUp() {
        try {
            File db = new File("./src/test/db/");
            if (db.exists()) {
                FileUtils.cleanDirectory(db);
            }
        } catch (IOException e) {
            Assert.fail("Failed to delete Hsqldb's db dir at ./src/test/db");
        }
    }
}
