package org.umlg.sqlg.test;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;

import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

/**
 * Date: 2014/07/13
 * Time: 5:23 PM
 */
public class TestLoadElementProperties extends BaseTest {

    @Test
    public void testLoadVertexProperties() {
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        this.sqlgGraph.tx().commit();
        marko = this.sqlgGraph.traversal().V(marko.id()).next();
        Assert.assertEquals("marko", marko.property("name").value());
    }

    //This will try to load the property before setting it.
    @Test
    public void testLoadVertexPropertyProperly() {
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        this.sqlgGraph.tx().commit();
        Vertex john = this.sqlgGraph.addVertex(T.label, "Person");
        john.property("name", "john");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("Person").count().next().intValue());
        Assert.assertEquals("marko", this.sqlgGraph.traversal().V(marko).next().value("name"));
        Assert.assertEquals("john", this.sqlgGraph.traversal().V(john).next().value("name"));
    }

    //This will try to load the property before setting it.
    @Test
    public void testLoadVertexPropertyProperlyUserSuppliedPK() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "Person",
                        new HashMap<>() {{
                            put("uid", PropertyType.varChar(100));
                            put("name", PropertyType.STRING);
                        }},
                        ListOrderedSet.listOrderedSet(Collections.singletonList("uid"))
                );
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "marko");
        this.sqlgGraph.tx().commit();
        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString());
        john.property("name", "john");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("Person").count().next().intValue());
        Assert.assertEquals("marko", this.sqlgGraph.traversal().V(marko).next().value("name"));
        Assert.assertEquals("john", this.sqlgGraph.traversal().V(john).next().value("name"));
    }

    @Test
    public void testLoadEdgeProperties() {
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        Edge friend = marko.addEdge("friend", john, "weight", 1);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.traversal().E(friend.id()).next().property("weight").value());
    }

}
