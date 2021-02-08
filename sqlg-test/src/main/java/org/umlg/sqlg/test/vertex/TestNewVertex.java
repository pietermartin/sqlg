package org.umlg.sqlg.test.vertex;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgExceptions;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2014/10/04
 * Time: 10:17 AM
 */
public class TestNewVertex extends BaseTest {

    @Test
    public void testNewVertexDoesNotQueryLabels() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john1");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john2");
        Edge e1 = v1.addEdge("friend", v2, "weight", 1);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().count().next().intValue());
        Assert.assertEquals(1, this.sqlgGraph.traversal().E().count().next().intValue());
        Assert.assertEquals(v1, this.sqlgGraph.traversal().V(v1.id()).next());
        Assert.assertEquals(v2, this.sqlgGraph.traversal().V(v2.id()).next());
        Assert.assertEquals(e1, this.sqlgGraph.traversal().E(e1.id()).next());
        this.sqlgGraph.tx().commit();
    }

    @Test
    public void testNewVertex() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john1");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john2");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().count().next().intValue());
    }

    @Test(expected = SqlgExceptions.InvalidIdException.class)
    public void testInvalidId() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john1");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john2");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.traversal().V("asdasdasd").next();
    }

    @Test
    public void testEmpty(){
    	Vertex person1 = this.sqlgGraph.addVertex(T.label, "Empty","empty","");
    	this.sqlgGraph.tx().commit();
    	Assert.assertNotNull(person1.id());
    	Object o=this.sqlgGraph.traversal().V().hasLabel("Empty").values("empty").next();
    	Assert.assertEquals("",o);
    }
}
