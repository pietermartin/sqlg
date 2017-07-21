package org.umlg.sqlg.test.tp3.structure;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructurePerformanceSuite;
import org.junit.runner.RunWith;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.tp3.SqlgMSSqlServerProvider;

/**
 * Executes the Gremlin Structure Performance Test Suite using SqlgGraph on SQL Server.
 */
@RunWith(StructurePerformanceSuite.class)
@GraphProviderClass(provider = SqlgMSSqlServerProvider.class, graph = SqlgGraph.class)
public class SqlgMSSqlServerGroovyStructurePerformanceTest {

}
