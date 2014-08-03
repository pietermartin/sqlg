package org.umlg.sqlg.test.tp3.structure;

import com.tinkerpop.gremlin.structure.StructurePerformanceSuite;
import org.junit.runner.RunWith;
import org.umlg.sqlg.test.tp3.SqlGPostgresProvider;

/**
 * Executes the Gremlin Structure Performance Test Suite using TinkerGraph.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(StructurePerformanceSuite.class)
@StructurePerformanceSuite.GraphProviderClass(SqlGPostgresProvider.class)
public class SqlGPostgresStructurePerformanceTest {

}