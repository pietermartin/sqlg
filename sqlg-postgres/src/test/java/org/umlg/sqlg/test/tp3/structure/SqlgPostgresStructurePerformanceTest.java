package org.umlg.sqlg.test.tp3.structure;

import com.tinkerpop.gremlin.structure.StructurePerformanceSuite;
import org.junit.runner.RunWith;
import org.umlg.sqlg.structure.SqlG;
import org.umlg.sqlg.test.tp3.SqlgPostgresProvider;

/**
 * Executes the Gremlin Structure Performance Test Suite using TinkerGraph.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(StructurePerformanceSuite.class)
@StructurePerformanceSuite.GraphProviderClass(provider = SqlgPostgresProvider.class, graph = SqlG.class)
//@StructurePerformanceSuite.GraphProviderClass(SqlgPostgresProvider.class)
public class SqlgPostgresStructurePerformanceTest {

}