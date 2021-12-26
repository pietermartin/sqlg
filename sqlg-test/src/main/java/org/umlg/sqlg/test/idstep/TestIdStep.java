package org.umlg.sqlg.test.idstep;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class TestIdStep extends BaseTest {

    @Test
    public void testId() {
        this.sqlgGraph.addVertex(T.label, "A", "name", "what1");
        this.sqlgGraph.addVertex(T.label, "A", "name", "what2");
        this.sqlgGraph.addVertex(T.label, "A", "name", "what3");
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Object> traversal = this.sqlgGraph.traversal().V().hasLabel("A").id();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_A\".\"ID\" AS \"alias1\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\"", sql);
        } else if (isMariaDb()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\t`PUBLIC`.`V_A`.`ID` AS `alias1`\n" +
                    "FROM\n" +
                    "\t`PUBLIC`.`V_A`", sql);
        } else if (isHsqldb() || isH2()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\t\"PUBLIC\".\"V_A\".\"ID\" AS \"alias1\"\n" +
                    "FROM\n" +
                    "\t\"PUBLIC\".\"V_A\"", sql);

        }
        List<Object> recordIdList = traversal.toList();
        Assert.assertEquals(3, recordIdList.size());
        Assert.assertTrue(recordIdList.get(0) instanceof RecordId);
        Assert.assertTrue(recordIdList.get(1) instanceof RecordId);
        Assert.assertTrue(recordIdList.get(2) instanceof RecordId);
    }

    @Test
    public void testIdOnEdge() {
        Vertex aVertex = this.sqlgGraph.addVertex(T.label, "A", "name", "halo");
        Vertex bVertex = this.sqlgGraph.addVertex(T.label, "B", "name", "halo");
        Edge edge = aVertex.addEdge("ab", bVertex, "name", "halo");
        this.sqlgGraph.tx().commit();

        Traversal<Edge, Object> traversal = this.sqlgGraph.traversal().E().hasId(edge.id()).id();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\t\"public\".\"E_ab\".\"public.A__O\" AS \"alias1\",\n" +
                    "\t\"public\".\"E_ab\".\"public.B__I\" AS \"alias2\",\n" +
                    "\t\"public\".\"E_ab\".\"ID\" AS \"alias3\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"E_ab\"\n" +
                    "WHERE\n" +
                    "\t( \"public\".\"E_ab\".\"ID\" = ?)", sql);
        } else if (isHsqldb() || isH2()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\t\"PUBLIC\".\"E_ab\".\"PUBLIC.B__I\" AS \"alias1\",\n" +
                    "\t\"PUBLIC\".\"E_ab\".\"PUBLIC.A__O\" AS \"alias2\",\n" +
                    "\t\"PUBLIC\".\"E_ab\".\"ID\" AS \"alias3\"\n" +
                    "FROM\n" +
                    "\t\"PUBLIC\".\"E_ab\"\n" +
                    "WHERE\n" +
                    "\t( \"PUBLIC\".\"E_ab\".\"ID\" = ?)", sql);
        } else if (isMariaDb()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\t`PUBLIC`.`E_ab`.`PUBLIC.B__I` AS `alias1`,\n" +
                    "\t`PUBLIC`.`E_ab`.`PUBLIC.A__O` AS `alias2`,\n" +
                    "\t`PUBLIC`.`E_ab`.`ID` AS `alias3`\n" +
                    "FROM\n" +
                    "\t`PUBLIC`.`E_ab`\n" +
                    "WHERE\n" +
                    "\t( `PUBLIC`.`E_ab`.`ID` = ?) COLLATE latin1_general_cs", sql);
        }
        List<Object> recordIdList = traversal.toList();
        Assert.assertEquals(1, recordIdList.size());
        Assert.assertTrue(recordIdList.get(0) instanceof RecordId);

        Traversal<Vertex, Object> traversalAgain = this.sqlgGraph.traversal().V().hasLabel("A").outE().id();
        sql = getSQL(traversalAgain);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\t\"public\".\"E_ab\".\"public.A__O\" AS \"alias1\",\n" +
                    "\t\"public\".\"E_ab\".\"public.B__I\" AS \"alias2\",\n" +
                    "\t\"public\".\"E_ab\".\"ID\" AS \"alias3\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_ab\" ON \"public\".\"V_A\".\"ID\" = \"public\".\"E_ab\".\"public.A__O\"", sql);

        }
        recordIdList = traversalAgain.toList();
        Assert.assertEquals(1, recordIdList.size());
        Assert.assertTrue(recordIdList.get(0) instanceof RecordId);
    }

    @Test
    public void testNoProperties() {
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        Traversal<Vertex, Integer> traversal = this.sqlgGraph.traversal().V().values("age").max();
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testIdentifiers() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new HashMap<>() {{
                    put("uuid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(10));
                }},
                ListOrderedSet.listOrderedSet(List.of("uuid"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new HashMap<>() {{
                    put("uuid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(10));
                }},
                ListOrderedSet.listOrderedSet(List.of("uuid"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new HashMap<>() {{
                    put("uuid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(10));
                }},
                ListOrderedSet.listOrderedSet(List.of("uuid"))
        );
        this.sqlgGraph.tx().commit();

        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "uuid", UUID.randomUUID().toString(), "name", "haloA");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "uuid", UUID.randomUUID().toString(), "name", "haloB1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "uuid", UUID.randomUUID().toString(), "name", "haloB2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "uuid", UUID.randomUUID().toString(), "name", "haloB3");
        a.addEdge("ab", b1, "uuid", UUID.randomUUID().toString(), "name", "edge1");
        a.addEdge("ab", b2, "uuid", UUID.randomUUID().toString(), "name", "edge2");
        a.addEdge("ab", b3, "uuid", UUID.randomUUID().toString(), "name", "edge3");

        Traversal<Vertex, Object> traversal = this.sqlgGraph.traversal().V().hasLabel("A").id();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_A\".\"uuid\" AS \"alias1\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\"", sql);
        } else if (isHsqldb() || isH2()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\t\"PUBLIC\".\"V_A\".\"uuid\" AS \"alias1\"\n" +
                    "FROM\n" +
                    "\t\"PUBLIC\".\"V_A\"", sql);
        } else if (isMariaDb()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\t`PUBLIC`.`V_A`.`uuid` AS `alias1`\n" +
                    "FROM\n" +
                    "\t`PUBLIC`.`V_A`", sql);
        }
        List<Object> recordIds = traversal.toList();
        Assert.assertEquals(1, recordIds.size());

        traversal = this.sqlgGraph.traversal().V().hasLabel("A").outE().id();
        sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\t\"public\".\"E_ab\".\"public.A.uuid__O\" AS \"alias1\",\n" +
                    "\t\"public\".\"E_ab\".\"public.B.uuid__I\" AS \"alias2\",\n" +
                    "\t\"public\".\"E_ab\".\"uuid\" AS \"alias3\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_ab\" ON \"public\".\"V_A\".\"uuid\" = \"public\".\"E_ab\".\"public.A.uuid__O\"", sql);
        } else if (isHsqldb() || isH2()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\t\"PUBLIC\".\"E_ab\".\"PUBLIC.B.uuid__I\" AS \"alias1\",\n" +
                    "\t\"PUBLIC\".\"E_ab\".\"PUBLIC.A.uuid__O\" AS \"alias2\",\n" +
                    "\t\"PUBLIC\".\"E_ab\".\"uuid\" AS \"alias3\"\n" +
                    "FROM\n" +
                    "\t\"PUBLIC\".\"V_A\" INNER JOIN\n" +
                    "\t\"PUBLIC\".\"E_ab\" ON \"PUBLIC\".\"V_A\".\"uuid\" = \"PUBLIC\".\"E_ab\".\"PUBLIC.A.uuid__O\"", sql);
        } else if (isMariaDb()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\t`PUBLIC`.`E_ab`.`PUBLIC.B.uuid__I` AS `alias1`,\n" +
                    "\t`PUBLIC`.`E_ab`.`PUBLIC.A.uuid__O` AS `alias2`,\n" +
                    "\t`PUBLIC`.`E_ab`.`uuid` AS `alias3`\n" +
                    "FROM\n" +
                    "\t`PUBLIC`.`V_A` INNER JOIN\n" +
                    "\t`PUBLIC`.`E_ab` ON `PUBLIC`.`V_A`.`uuid` = `PUBLIC`.`E_ab`.`PUBLIC.A.uuid__O`", sql);
        }
        recordIds = traversal.toList();
        Assert.assertEquals(3, recordIds.size());
        Assert.assertTrue(recordIds.stream().map(r -> (RecordId)r).allMatch(r -> r.getSchemaTable().getTable().equals("ab")));
    }
}
