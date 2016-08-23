package org.umlg.sqlg.test.tp3.groovy;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.groovy.GroovyEnvironmentIntegrateSuite;
import org.junit.runner.RunWith;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.tp3.SqlgH2Provider;

@RunWith(GroovyEnvironmentIntegrateSuite.class)
@GraphProviderClass(provider = SqlgH2Provider.class, graph = SqlgGraph.class)
public class SqlgH2GroovyEnvironmentIntegrateTest {
}
