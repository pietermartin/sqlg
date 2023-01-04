package org.umlg.sqlg.test.reducing;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.Map;

/**
 * @author <a href="https://github.com/pietermartin">Pieter Martin</a>
 *         Date: 2017/05/02
 */
public class TestGroupCount extends BaseTest {

    @Test
    public void g_V_both_groupCountXaX_out_capXaX_selectXkeysX_unfold_both_groupCountXaX_capXaX() {
        loadModern();

        final Traversal<Vertex, Map<Vertex, Long>> traversal = this.sqlgGraph.traversal().V().both().groupCount("a").out().cap("a").select(Column.keys).unfold().both().groupCount("a").cap("a");
        printTraversalForm(traversal);
        //  [{v[1]=6, v[2]=2, v[3]=6, v[4]=6, v[5]=2, v[6]=2}]
        final Map<Vertex, Long> map = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertEquals(6, map.size());
        Assert.assertEquals(6L, map.get(convertToVertex(this.sqlgGraph, "marko")).longValue());
        Assert.assertEquals(2L, map.get(convertToVertex(this.sqlgGraph, "vadas")).longValue());
        Assert.assertEquals(6L, map.get(convertToVertex(this.sqlgGraph, "lop")).longValue());
        Assert.assertEquals(6L, map.get(convertToVertex(this.sqlgGraph, "josh")).longValue());
        Assert.assertEquals(2L, map.get(convertToVertex(this.sqlgGraph, "ripple")).longValue());
        Assert.assertEquals(6L, map.get(convertToVertex(this.sqlgGraph, "marko")).longValue());
//        checkSideEffects(traversal.asAdmin().getSideEffects(), "a", HashMap.class);
    }
    
    @Test
    public void testGroupCountByLabel(){
    	loadModern();
    	final Traversal<Vertex, Map<Object, Long>> traversal = this.sqlgGraph.traversal().V().groupCount().by(T.label);
    	printTraversalForm(traversal);
    	Assert.assertTrue(traversal.hasNext());
    	Map<Object, Long> map=traversal.next();
    	Assert.assertFalse(traversal.hasNext());
        Assert.assertEquals(2, map.size());
        Assert.assertEquals(Long.valueOf(4),map.get("person"));
        Assert.assertEquals(Long.valueOf(2),map.get("software"));
    }
}
