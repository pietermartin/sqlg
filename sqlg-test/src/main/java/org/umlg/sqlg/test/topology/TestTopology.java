package org.umlg.sqlg.test.topology;

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
import org.umlg.sqlg.structure.TopologyInf;
import org.umlg.sqlg.structure.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Date: 2016/06/27
 * Time: 1:40 PM
 */
public class TestTopology extends BaseTest {

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
        List<Vertex> vertices = this.sqlgGraph.topology().V().hasLabel("sqlg_schema.schema").out("schema_vertex").order().by("name", Order.decr).toList();
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
    public void testTopologyWithoutFilter() {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "asd");
        this.sqlgGraph.addVertex(T.label, "Dog", "name", "asd");
        this.sqlgGraph.addVertex(T.label, "Cat", "name", "asd");
        this.sqlgGraph.tx().commit();
        Optional<VertexLabel> personVertexLabelOptional = this.sqlgGraph.getTopology().getVertexLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(),"Person");
        Assert.assertTrue(personVertexLabelOptional.isPresent());
        Map<String, Map<String, PropertyType>> labelAndProperties = this.sqlgGraph.getTopology().getAllTablesWithout(new HashSet<TopologyInf>() {{
            add(personVertexLabelOptional.get());

        }});
        Assert.assertEquals(2, labelAndProperties.size());
        Assert.assertTrue(labelAndProperties.containsKey(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_Dog"));
        Assert.assertTrue(labelAndProperties.containsKey(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_Cat"));
        Assert.assertTrue(labelAndProperties.get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_Dog").containsKey("name"));
        Assert.assertTrue(labelAndProperties.get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_Cat").containsKey("name"));
        Assert.assertTrue(labelAndProperties.get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_Dog").get("name") == PropertyType.STRING);
        Assert.assertTrue(labelAndProperties.get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_Cat").get("name") == PropertyType.STRING);
    }

}
