package org.umlg.sqlg.test.tp3;

import com.tinkerpop.gremlin.structure.StructurePerformanceSuite;
import org.junit.runner.RunWith;
import org.umlg.sqlg.structure.SqlgGraph;

/**
 * Executes the Gremlin Structure Performance Test Suite using TinkerGraph.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(StructurePerformanceSuite.class)
@StructurePerformanceSuite.GraphProviderClass(provider = SqlGMariaDBProvider.class, graph = SqlgGraph.class)
public class SqlgGraphMariaDBStructurePerformanceTest {

}