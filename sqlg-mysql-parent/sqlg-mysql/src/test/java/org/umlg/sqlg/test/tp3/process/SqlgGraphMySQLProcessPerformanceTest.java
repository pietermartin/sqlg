package org.umlg.sqlg.test.tp3.process;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessPerformanceSuite;
import org.junit.runner.RunWith;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.tp3.SqlgMySQLProvider;

/**
 * Date: 2015/05/18
 * Time: 8:10 AM
 */
@RunWith(ProcessPerformanceSuite.class)
@GraphProviderClass(provider = SqlgMySQLProvider.class, graph = SqlgGraph.class)
public class SqlgGraphMySQLProcessPerformanceTest {
}
