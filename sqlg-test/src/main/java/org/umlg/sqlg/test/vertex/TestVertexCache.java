package org.umlg.sqlg.test.vertex;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.Topology;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.net.URL;
import java.util.*;

/**
 * Date: 2014/10/04
 * Time: 2:03 PM
 */
public class TestVertexCache extends BaseTest {

    @BeforeClass
    public static void beforeClass() {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            Configurations configs = new Configurations();
            configuration = configs.properties(sqlProperties);
            configuration.setProperty("cache.vertices", true);
            if (!configuration.containsKey("jdbc.url")) {
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));
            }
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testVertexCacheWithIdentifiers() {
        // This is testing a very specific case that previously failed:
        //
        // - cache.vertices is true
        // - identifiers are being used for vertices
        // - a given edge label can be between multiple vertex labels
        // - bothE().otherV() is used in a subsequent traversal

        Topology topology = this.sqlgGraph.getTopology();

        Map<String, PropertyType> vertexProperties = new HashMap<>();
        vertexProperties.put("id", PropertyType.varChar(10));
        vertexProperties.put("name", PropertyType.STRING);

        ListOrderedSet<String> identifiers = ListOrderedSet.listOrderedSet(Collections.singletonList("id"));

        VertexLabel vertexLabelA = topology.ensureVertexLabelExist("A", vertexProperties, identifiers);
        VertexLabel vertexLabelB = topology.ensureVertexLabelExist("B", vertexProperties, identifiers);
        VertexLabel vertexLabelC = topology.ensureVertexLabelExist("C", vertexProperties, identifiers);

        Map<String, PropertyType> edgeProperties = new HashMap<>();
        //MariaDb does seem to consider 'ID" and 'id' as the same in 'CREATE' statement.
        //Change it to _id
        edgeProperties.put("_id", PropertyType.varChar(10));
        edgeProperties.put("how", PropertyType.STRING);

        topology.ensureEdgeLabelExist("related", vertexLabelA, vertexLabelB, edgeProperties);
        topology.ensureEdgeLabelExist("related", vertexLabelA, vertexLabelC, edgeProperties);

        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "id", "1", "name", "joe");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "B", "id", "2", "name", "frank");

        v1.addEdge("related", v2, "how", "friend");

        // Without the fix, this will trigger a NPE in RecordId.ID.hashCode() because "identifiers" contains a null.
        Vertex v2a = sqlgGraph.traversal().V().has("id", "1").bothE().otherV().next();
        Assert.assertEquals(v2, v2a);
        v2a = sqlgGraph.traversal().V(v1).outE().otherV().next();
        Assert.assertEquals(v2, v2a);
    }

    @Test
    public void testVertexTransactionalCache() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person");
        v1.addEdge("friend", v2);
        Assert.assertEquals(1, vertexTraversal(this.sqlgGraph, v1).out("friend").count().next().intValue());
        Vertex tmpV1 = this.sqlgGraph.traversal().V(v1.id()).next();
        tmpV1.addEdge("foe", v3);
        //this should fail as v1's out edges will not be updated
        Assert.assertEquals(1, vertexTraversal(this.sqlgGraph, tmpV1).out("foe").count().next().intValue());
        Assert.assertEquals(1, vertexTraversal(this.sqlgGraph, v1).out("foe").count().next().intValue());
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testVertexTransactionalCache2() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person");
        Edge e1 = v1.addEdge("friend", v2);
        Assert.assertEquals(1, vertexTraversal(this.sqlgGraph, v1).out("friend").count().next().intValue());

        Vertex tmpV1 = edgeTraversal(this.sqlgGraph, this.sqlgGraph.traversal().E(e1.id()).next()).outV().next();
        tmpV1.addEdge("foe", v3);
        //this should fail as v1's out edges will not be updated
        Assert.assertEquals(1, vertexTraversal(this.sqlgGraph, tmpV1).out("foe").count().next().intValue());
        Assert.assertEquals(1, vertexTraversal(this.sqlgGraph, v1).out("foe").count().next().intValue());
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testVertexTransactionalCache3() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person");
        Edge e1 = v1.addEdge("friend", v2);
        Assert.assertEquals(1, vertexTraversal(this.sqlgGraph, v1).out("friend").count().next().intValue());

        Vertex tmpV1 = vertexTraversal(this.sqlgGraph, v1).outE("friend").outV().next();
        tmpV1.addEdge("foe", v3);
        //this should fail as v1's out edges will not be updated
        Assert.assertEquals(1, vertexTraversal(this.sqlgGraph, tmpV1).out("foe").count().next().intValue());
        Assert.assertEquals(1, vertexTraversal(this.sqlgGraph, v1).out("foe").count().next().intValue());
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testMultipleReferencesToSameVertex() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals("john", v1.value("name"));
        //_v1 is in the transaction cache
        //v1 is not
        Vertex _v1 = this.sqlgGraph.traversal().V(v1.id()).next();
        Assert.assertEquals("john", _v1.value("name"));
        v1.property("name", "john1");
        Assert.assertEquals("john1", v1.value("name"));
        Assert.assertEquals("john1", _v1.value("name"));
    }

    @Test
    public void testMultipleReferencesToSameVertexUserSuppliedPK() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "Person",
                        new HashMap<String, PropertyType>() {{
                            put("uid1", PropertyType.varChar(100));
                            put("uid2", PropertyType.varChar(100));
                            put("name", PropertyType.STRING);
                        }},
                        ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
                );
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();
        Assert.assertEquals("john", v1.value("name"));
        //_v1 is in the transaction cache
        //v1 is not
        Vertex _v1 = this.sqlgGraph.traversal().V(v1.id()).next();
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
        Vertex _v1 = this.sqlgGraph.traversal().V(v1.id()).next();
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

        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
        List<Vertex> cars = vertexTraversal(this.sqlgGraph, v1).out("car").toList();
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
