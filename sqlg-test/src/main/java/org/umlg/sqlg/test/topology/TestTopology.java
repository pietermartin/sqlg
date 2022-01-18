package org.umlg.sqlg.test.topology;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.Topology;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;

/**
 * Date: 2016/06/27
 * Time: 1:40 PM
 */
public class TestTopology extends BaseTest {

    @Test
    public void testDotInLabelName() {
        Vertex hand = this.sqlgGraph.addVertex(T.label, "A.A", "name", "a");
        Vertex finger = this.sqlgGraph.addVertex(T.label, "A.B.interface", "name", "b");
        hand.addEdge("a_b", finger);
        this.sqlgGraph.tx().commit();

        List<Vertex> children = sqlgGraph.traversal().V(hand)
                .out("a_b")
                .toList();
        Assert.assertEquals(1, children.size());
    }

    @Test
    public void testDotInLabelNameUserSuppliedIdentifiers() {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        VertexLabel testVertexLabel = aSchema.ensureVertexLabelExist(
                "TestA",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name1", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid"))
        );
        VertexLabel testTestVertexLabel = aSchema.ensureVertexLabelExist(
                "TestB",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name1", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid"))
        );
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A.TestA", "uid", UUID.randomUUID().toString(), "name1", "a");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A.Test.TestA", "uid", UUID.randomUUID().toString(), "name1", "a");
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.getTopology().ensureEdgeLabelExist(
                "e1",
                testVertexLabel,
                testTestVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name1", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid"))
        );
        this.sqlgGraph.tx().commit();
        v1.addEdge("e1", v2, "uid", UUID.randomUUID().toString(), "name1", "a");
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A.TestA").out().toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v2, vertices.get(0));
    }

    @Test
    public void failTest() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "MySchema.A", "name", "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "MySchema.B", "name", "B");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        //works
        Assert.assertEquals(2,
                this.sqlgGraph.topology()
                        .V().hasLabel("sqlg_schema.schema").has("name", P.within("MySchema")).as("schema").values("name").as("schemaName").select("schema")
                        .out("schema_vertex")
                        .count().next().intValue());

        //fails, was bug no longer fails
        Assert.assertTrue(
                this.sqlgGraph.topology()
                        .V().hasLabel("sqlg_schema.schema").has("name", P.within("MySchema")).as("schema").values("name").as("schemaName").select("schema")
                        .out("schema_vertex")
                        .hasNext());
    }

    @Test
    public void testTopologyTraversalWithOrderBy() {
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "B");
        this.sqlgGraph.addVertex(T.label, "C");
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.topology().V().hasLabel("sqlg_schema.schema").out("schema_vertex").order().by("name", Order.desc).toList();
        Assert.assertEquals("A", vertices.get(2).value("name"));
        Assert.assertEquals("B", vertices.get(1).value("name"));
        Assert.assertEquals("C", vertices.get(0).value("name"));
    }

    @Test
    public void testTopologyTraversal() {
        Vertex gis = this.sqlgGraph.addVertex(T.label, "Gis", "name", "HaloGis1");
        Vertex something = this.sqlgGraph.addVertex(T.label, "Something", "name", "Something1");
        gis.addEdge("testEdge", something, "edgeProperty", "asdasd");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(2, this.sqlgGraph.topology().V().hasLabel("sqlg_schema.schema").out("schema_vertex").count().next().intValue());
        Assert.assertEquals(2, this.sqlgGraph.topology().V().hasLabel("sqlg_schema.vertex").in("schema_vertex").count().next().intValue());
        Assert.assertEquals(2, this.sqlgGraph.topology().V().hasLabel("sqlg_schema.vertex").out("vertex_property").count().next().intValue());
        Assert.assertEquals(2, this.sqlgGraph.topology().V().hasLabel("sqlg_schema.property").in("vertex_property").count().next().intValue());
        Assert.assertEquals(1, this.sqlgGraph.topology().V().hasLabel("sqlg_schema.property").in("edge_property").count().next().intValue());

        Vertex v = this.sqlgGraph.topology().V().hasLabel("sqlg_schema.schema").has("name", "public").next();
        Assert.assertTrue(v.edges(Direction.OUT, "schema_vertex").hasNext());

        Assert.assertEquals(2, this.sqlgGraph.topology().V().hasLabel("sqlg_schema.schema").as("schema").select("schema").out("schema_vertex").count().next().intValue());
        Assert.assertEquals(2, this.sqlgGraph.topology().V().hasLabel("sqlg_schema.schema").as("schema").values("name").as("schemaName").select("schema").out("schema_vertex").count().next().intValue());
        Assert.assertTrue(this.sqlgGraph.topology().V().hasLabel("sqlg_schema.schema").as("schema").values("name").as("schemaName").select("schema").out("schema_vertex").hasNext());

        Assert.assertEquals("testEdge", this.sqlgGraph.topology().V().hasLabel("sqlg_schema.property").in("edge_property").values("name").next());
    }


    //    This test a bug in rollback on edges.
    @Test
    public void testRollback() {
        loadModern();
        final Traversal<Vertex, Edge> traversal = this.sqlgGraph.traversal().V().aggregate("x").as("a").select("x").unfold().addE("existsWith").to("a").property("time", "now");
        IteratorUtils.asList(traversal);
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testAddColumns(){
    	//Create Schema
        Topology topology = sqlgGraph.getTopology();
        topology.ensureSchemaExist("TEST");

        Map<String, Object> columns = new HashMap<>();
        columns.put("Test1", "");
        columns.put("Test2", "");

        //Add a vertex and remove to create columns
        Vertex v = sqlgGraph.addVertex("TEST" + "." + "TEST_Table", columns);
        v.remove();
        sqlgGraph.tx().commit();

        columns = new HashMap<>();
        columns.put("Test1", "T1");
        columns.put("Test2", "T2");

        //Add the data
        sqlgGraph.addVertex("TEST" + "." + "TEST_Table", columns);
        sqlgGraph.tx().commit();

        //Simulating second load
        //Remove the whole table label
        Optional<VertexLabel> tableVertexLabel = sqlgGraph.getTopology().getVertexLabel("TEST", "TEST_Table");
        tableVertexLabel.ifPresent(vertexLabel -> vertexLabel.remove(false));

        columns = new HashMap<>();
        columns.put("Test1", "");
        columns.put("Test2", "");
        columns.put("Test3", "");

        //Add a vertex with more columns than previously had
        v = sqlgGraph.addVertex("TEST" + "." + "TEST_Table", columns);
        v.remove();
        sqlgGraph.tx().commit();
    }
}
