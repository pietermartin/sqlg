package org.umlg.tinkerpop3.test.structure;

import com.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;
import org.umlg.tinkerpop3.test.SqlGraphProvider;


/**
 * Executes the Standard Gremlin Structure Test Suite using Neo4j.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(StructureStandardSuite.class)
@StructureStandardSuite.GraphProviderClass(SqlGraphProvider.class)
public class SqlGraphStructureStandardTest {
}
