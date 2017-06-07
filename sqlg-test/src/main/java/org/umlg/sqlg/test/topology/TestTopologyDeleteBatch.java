package org.umlg.sqlg.test.topology;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.structure.Schema;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

/**
 * test deletion behavior in a batch
 * @author JP Moresmau
 *
 */
public class TestTopologyDeleteBatch extends BaseTest {

	@Test
	public void testSchemaDelete() throws Exception {
		String schema="willDelete";
		Vertex v1=sqlgGraph.addVertex(T.label,schema+".t1","name","n1","hello", "world");
		sqlgGraph.tx().commit();
		Configuration c= getConfigurationClone();
		c.setProperty(SqlgGraph.DISTRIBUTED, true);
		sqlgGraph=SqlgGraph.open(c);
		sqlgGraph.getTopology().getSchema(schema).ifPresent((Schema s)->s.remove(false));
		sqlgGraph.tx().commit();
		
		v1=sqlgGraph.addVertex(T.label,schema+".t1","name","n1");
		Vertex v2=sqlgGraph.addVertex(T.label,schema+".t2","name","n2");
		Edge e1 = v1.addEdge("e1", v2);
		sqlgGraph.tx().commit();
		
		sqlgGraph.tx().normalBatchModeOn();
		v1.property("hello", "world");
		
		e1.property("hello", "world");
		
		//sqlgGraph1.getTopology().getSchema(schema).ifPresent((Schema s)->s.remove(false));
		sqlgGraph.tx().commit();
		
	}
}
