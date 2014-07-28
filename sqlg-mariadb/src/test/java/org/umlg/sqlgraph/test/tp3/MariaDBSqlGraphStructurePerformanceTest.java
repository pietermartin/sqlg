package org.umlg.sqlgraph.test.tp3;

import com.tinkerpop.gremlin.structure.StructurePerformanceSuite;
import org.junit.runner.RunWith;

/**
 * Executes the Gremlin Structure Performance Test Suite using TinkerGraph.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(StructurePerformanceSuite.class)
@StructurePerformanceSuite.GraphProviderClass(SqlGraphProvider.class)
public class MariaDBSqlGraphStructurePerformanceTest {

}