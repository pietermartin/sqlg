package org.umlg.sqlg.test.vertex;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.net.URL;
import java.util.List;

/**
 * Date: 2014/10/04
 * Time: 2:03 PM
 */
public class TestVertexCache extends BaseTest {

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            configuration = new PropertiesConfiguration(sqlProperties);
            configuration.setProperty("cache.vertices", true);
            if (!configuration.containsKey("jdbc.url")) {
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));
            }
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testVertexTransactionalCache() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person");
        v1.addEdge("friend", v2);
        Assert.assertEquals(1, vertexTraversal(v1).out("friend").count().next().intValue());
        Vertex tmpV1 = this.sqlgGraph.v(v1.id());
        tmpV1.addEdge("foe", v3);
        //this should fail as v1's out edges will not be updated
        Assert.assertEquals(1, vertexTraversal(tmpV1).out("foe").count().next().intValue());
        Assert.assertEquals(1, vertexTraversal(v1).out("foe").count().next().intValue());
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testVertexTransactionalCache2() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person");
        Edge e1 = v1.addEdge("friend", v2);
        Assert.assertEquals(1, vertexTraversal(v1).out("friend").count().next().intValue());

        Vertex tmpV1 = edgeTraversal(this.sqlgGraph.e(e1.id())).outV().next();
        tmpV1.addEdge("foe", v3);
        //this should fail as v1's out edges will not be updated
        Assert.assertEquals(1, vertexTraversal(tmpV1).out("foe").count().next().intValue());
        Assert.assertEquals(1, vertexTraversal(v1).out("foe").count().next().intValue());
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testVertexTransactionalCache3() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person");
        Edge e1 = v1.addEdge("friend", v2);
        Assert.assertEquals(1, vertexTraversal(v1).out("friend").count().next().intValue());

        Vertex tmpV1 = vertexTraversal(v1).outE("friend").outV().next();
        tmpV1.addEdge("foe", v3);
        //this should fail as v1's out edges will not be updated
        Assert.assertEquals(1, vertexTraversal(tmpV1).out("foe").count().next().intValue());
        Assert.assertEquals(1, vertexTraversal(v1).out("foe").count().next().intValue());
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testMultipleReferencesToSameVertex() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals("john", v1.value("name"));
        //_v1 is in the transaction cache
        //v1 is not
        Vertex _v1 = this.sqlgGraph.v(v1.id());
        Assert.assertEquals("john", _v1.value("name"));
        v1.property("name", "john1");
        Assert.assertEquals("john1", v1.value("name"));
        Assert.assertEquals("john1", _v1.value("name"));
    }

    @Test
    public void testMultipleReferencesToSameVertex2Instances() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        this.sqlgGraph.tx().commit();
        //_v1 is in the transaction cache
        //v1 is not
        Vertex _v1 = this.sqlgGraph.v(v1.id());
        Assert.assertEquals("john", v1.value("name"));
        Assert.assertEquals("john", _v1.value("name"));
        v1.property("name", "john1");
        Assert.assertEquals("john1", v1.value("name"));
        Assert.assertEquals("john1", _v1.value("name"));
    }

    @Test
    public void testPropertiesNotBeingCachedOnVertexOut() {

        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Car", "name", "a");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Car", "name", "b");
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "Car", "name", "c");

        v1.addEdge("car", v2);
        v1.addEdge("car", v3);
        v1.addEdge("car", v4);

        this.sqlgGraph.tx().commit();

        v1 = this.sqlgGraph.v(v1.id());
        List<Vertex> cars = vertexTraversal(v1).out("car").toList();
        Assert.assertEquals(3, cars.size());

    }

    @Test
    public void testMultipleThreadsAccessSameVertexInstance() throws InterruptedException {
        final Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        this.sqlgGraph.tx().commit();
        Thread t = new Thread(() -> {
            Vertex b = TestVertexCache.this.sqlgGraph.addVertex(T.label, "Person");
            v1.property("name", "john1");
            TestVertexCache.this.sqlgGraph.tx().commit();
        });
        t.start();
        t.join();
        Assert.assertEquals("john1", v1.<String>value("name"));
    }
}
