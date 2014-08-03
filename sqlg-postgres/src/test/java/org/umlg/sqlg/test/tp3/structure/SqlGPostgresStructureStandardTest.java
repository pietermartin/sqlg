package org.umlg.sqlg.test.tp3.structure;

import com.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;
import org.umlg.sqlg.test.tp3.SqlGPostgresProvider;


/**
 * Executes the Standard Gremlin Structure Test Suite using Neo4j.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(StructureStandardSuite.class)
@StructureStandardSuite.GraphProviderClass(SqlGPostgresProvider.class)
public class SqlGPostgresStructureStandardTest {
}
