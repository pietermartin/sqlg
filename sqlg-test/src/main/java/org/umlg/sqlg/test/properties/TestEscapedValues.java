package org.umlg.sqlg.test.properties;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import static org.junit.Assert.assertEquals;

/**
 * Test values that are escaped by backslashes and that may impact SQL
 * @author jpmoresmau
 *
 */
public class TestEscapedValues extends BaseTest {

	@Test
	public void testEscapedValuesSingleQuery(){
		String[] vals = new String[] { "x-y", "x\ny", "x\"y", "x\\y", "x\\ny", "x\\\"y", "'x'y'" };
		for (String s : vals) {
			this.sqlgGraph.addVertex("Escaped").property("name", s);
		}
		this.sqlgGraph.tx().commit();
		for (String s : vals){
			assertEquals(s,1L,this.sqlgGraph.traversal().V().has("name",s).count().next().longValue());
			assertEquals(s,s,this.sqlgGraph.traversal().V().has("name",s).values("name").next());
		}
	}
	
	@Test
	public void testEscapedValuesWithinQuery(){
		String[] vals = new String[] { "x-y", "x\ny", "x\"y", "x\\y", "x\\ny", "x\\\"y", "'x'y'"  };
		for (String s : vals) {
			this.sqlgGraph.addVertex("Escaped").property("name", s); 
		}
		this.sqlgGraph.tx().commit();
				
		assertEquals(vals.length,1L,this.sqlgGraph.traversal().V().has("name",P.within(vals)).count().next().longValue());
				
	}
	
	@Test
	public void testEscapedValuesSingleQueryBatch(){
		Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
		this.sqlgGraph.tx().normalBatchModeOn();
		String[] vals = new String[] { "x-y", "x\ny", "x\"y", "x\\y", "x\\ny", "x\\\"y", "'x'y'" };
		for (String s : vals) {
			this.sqlgGraph.addVertex("Escaped").property("name", s);
		}
		this.sqlgGraph.tx().commit();
		for (String s : vals){
			assertEquals(s,1L,this.sqlgGraph.traversal().V().has("name",s).count().next().longValue());
			assertEquals(s,s,this.sqlgGraph.traversal().V().has("name",s).values("name").next());
		}
	}
}
