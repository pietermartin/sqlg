package org.umlg.sqlg.test.tp3.structure;

import com.tinkerpop.gremlin.structure.StructurePerformanceSuite;
import org.junit.runner.RunWith;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.tp3.SqlgPostgresProvider;

/**
 * Executes the Gremlin Structure Performance Test Suite using SqlG.
 */
@RunWith(StructurePerformanceSuite.class)
@StructurePerformanceSuite.GraphProviderClass(provider = SqlgPostgresProvider.class, graph = SqlgGraph.class)
public class SqlgPostgresStructurePerformanceTest {

}