package org.umlg.sqlg.test.properties;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * Test values that are escaped by backslashes and that may impact SQL
 *
 * @author jpmoresmau
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
			Assert.assertEquals(s,1L,this.sqlgGraph.traversal().V().has("name",s).count().next().longValue());
			Assert.assertEquals(s,s,this.sqlgGraph.traversal().V().has("name",s).values("name").next());
		}
	}

	@Test
	public void testEscapedValuesWithinQuery(){
		String[] vals = new String[] { "x-y", "x\ny", "x\"y", "x\\y", "x\\ny", "x\\\"y", "'x'y'"  };
		for (String s : vals) {
			this.sqlgGraph.addVertex("Escaped").property("name", s);
		}
		this.sqlgGraph.tx().commit();

		Assert.assertEquals(vals.length,1L,this.sqlgGraph.traversal().V().has("name", P.within(vals)).count().next().longValue());

	}

    @Test
    public void testEscapedValuesSingleQueryBatch() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
        this.sqlgGraph.tx().normalBatchModeOn();
        String[] vals = new String[]{"x-y", "x\ny", "x\"y", "x\\y", "x\\ny", "x\\\"y", "'x'y'", "x,y"};
        for (String s : vals) {
            this.sqlgGraph.addVertex("Escaped").property("name", s);
        }
        this.sqlgGraph.tx().commit();
        for (String s : vals) {
            Assert.assertEquals(s, 1L, this.sqlgGraph.traversal().V().has("name", s).count().next().longValue());
            Assert.assertEquals(s, s, this.sqlgGraph.traversal().V().has("name", s).values("name").next());
        }
    }

    @Test
    public void testEscapedValuesSingleQueryBatch2() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsStreamingBatchMode());
        this.sqlgGraph.tx().streamingBatchModeOn();
        this.sqlgGraph.streamVertex("Escaped", new LinkedHashMap<String, Object>() {{
            put("value", new String[]{"  {-45.53}H - LINK  "});
        }});
        this.sqlgGraph.tx().commit();
        List<String[]> result = this.sqlgGraph.traversal().V().<String[]>values("value").toList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(1, result.get(0).length);
        Assert.assertEquals("  {-45.53}H - LINK  ", result.get(0)[0]);
    }
}
