package org.umlg.sqlg.test.usersuppliedpk.topology;

import com.google.common.base.Preconditions;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.step.SqlgGraphStep;
import org.umlg.sqlg.step.SqlgPropertiesStep;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.Topology;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.net.URL;
import java.time.LocalDateTime;
import java.util.*;

import static org.umlg.sqlg.structure.topology.Topology.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/03/17
 */
@SuppressWarnings("unused")
public class TestUserSuppliedPKTopology extends BaseTest {

    @SuppressWarnings("Duplicates")
    @BeforeClass
    public static void beforeClass() {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            Configurations configs = new Configurations();
            configuration = configs.properties(sqlProperties);
            if (isPostgres()) {
                configuration.addProperty(SqlgGraph.DISTRIBUTED, true);
                if (!configuration.containsKey(SqlgGraph.JDBC_URL))
                    throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", SqlgGraph.JDBC_URL));
            }
        } catch (ConfigurationException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    public void testUserSuppliedIds() {
        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("surname", PropertyType.varChar(100));
                    put("nickname", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("name", "surname"))
        );
        personVertexLabel.ensureEdgeLabelExist(
                "marriedTo",
                personVertexLabel,
                new LinkedHashMap<>() {{
                    put("place", PropertyType.varChar(100));
                    put("when", PropertyType.LOCALDATETIME);
                }},
                ListOrderedSet.listOrderedSet(List.of("place", "when"))
        );
        this.sqlgGraph.tx().commit();

        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "John", "surname", "Longfellow", "nickname", "Longboy");
        Vertex sue = this.sqlgGraph.addVertex(T.label, "Person", "name", "Sue", "surname", "Pretty");
        john.addEdge("marriedTo", sue, "place", "Timbuck2", "when", LocalDateTime.now());
        this.sqlgGraph.tx().commit();

        List<Vertex> marriedTo = this.sqlgGraph.traversal().V().hasLabel("Person")
                .has("name", "John")
                .out("marriedTo")
                .toList();
        Assert.assertEquals(1, marriedTo.size());
        Assert.assertEquals(sue, marriedTo.get(0));
    }

    @Test
    public void testUserSuppliedLongKeys() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.LONG);
                    put("uid2", PropertyType.LONG);
                    put("name", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.INTEGER);
                    put("uid2", PropertyType.INTEGER);
                    put("name", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        this.sqlgGraph.tx().commit();
        Vertex vLong = this.sqlgGraph.addVertex(T.label, "A", "uid1", 1L, "uid2", 1L);
        Vertex vInteger = this.sqlgGraph.addVertex(T.label, "B", "uid1", 1, "uid2", 1);
        RecordId recordIdLong = (RecordId) vLong.id();
        RecordId recordIdInteger = (RecordId) vInteger.id();
        this.sqlgGraph.tx().commit();

        RecordId recordIdLong1 = RecordId.from(this.sqlgGraph, recordIdLong.toString());
        Assert.assertTrue(this.sqlgGraph.traversal().V(recordIdLong1).hasNext());
        RecordId recordIdInteger1 = RecordId.from(this.sqlgGraph, recordIdInteger.toString());
        Assert.assertTrue(this.sqlgGraph.traversal().V(recordIdInteger1).hasNext());
    }

    @Test
    public void testAddEdgeUserSuppliedPK() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel cVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "C",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "e1",
                bVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "e1",
                cVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        this.sqlgGraph.tx().commit();


        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "b1");
        a1.addEdge("e1", b1, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());

        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "a2");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "c2");
        a2.addEdge("e1", c2, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());

        this.sqlgGraph.tx().commit();


    }

    @Test
    public void testVertexCompositeIds() throws Exception {
        this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("surname", PropertyType.varChar(100));
                    put("country", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("name", "surname"))
        );

        Optional<VertexLabel> personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("Person");
        Assert.assertTrue(personVertexLabel.isPresent());
        Assert.assertEquals(2, personVertexLabel.get().getIdentifiers().size());
        Assert.assertEquals("name", personVertexLabel.get().getIdentifiers().get(0));
        Assert.assertEquals("surname", personVertexLabel.get().getIdentifiers().get(1));

        //HSQLDB does not support transactional schema creation.
        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            this.sqlgGraph.tx().rollback();
            personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("Person");
            Assert.assertFalse(personVertexLabel.isPresent());
            if (this.sqlgGraph.getSqlDialect().supportsDistribution()) {
                personVertexLabel = this.sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("Person");
                Assert.assertFalse(personVertexLabel.isPresent());
            }
        }

        this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("surname", PropertyType.varChar(100));
                    put("country", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("name", "surname"))
        );
        this.sqlgGraph.tx().commit();
        Thread.sleep(1000);

        personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("Person");
        Assert.assertTrue(personVertexLabel.isPresent());
        Assert.assertEquals(2, personVertexLabel.get().getIdentifiers().size());
        Assert.assertEquals("name", personVertexLabel.get().getIdentifiers().get(0));
        Assert.assertEquals("surname", personVertexLabel.get().getIdentifiers().get(1));

        if (isPostgres()) {
            personVertexLabel = this.sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("Person");
            Assert.assertTrue(personVertexLabel.isPresent());
            Assert.assertEquals(2, personVertexLabel.get().getIdentifiers().size());
            Assert.assertEquals("name", personVertexLabel.get().getIdentifiers().get(0));
            Assert.assertEquals("surname", personVertexLabel.get().getIdentifiers().get(1));
        }

        this.sqlgGraph.close();
        this.sqlgGraph = SqlgGraph.open(configuration);
        personVertexLabel = sqlgGraph.getTopology().getPublicSchema().getVertexLabel("Person");
        Assert.assertTrue(personVertexLabel.isPresent());
        Assert.assertEquals(2, personVertexLabel.get().getIdentifiers().size());
        Assert.assertEquals("name", personVertexLabel.get().getIdentifiers().get(0));
        Assert.assertEquals("surname", personVertexLabel.get().getIdentifiers().get(1));

        Thread.sleep(1000);
        dropSqlgSchema(sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph = SqlgGraph.open(configuration)) {
            personVertexLabel = sqlgGraph.getTopology().getPublicSchema().getVertexLabel("Person");
            Assert.assertTrue(personVertexLabel.isPresent());
            Assert.assertEquals(2, personVertexLabel.get().getIdentifiers().size());
            Assert.assertEquals("name", personVertexLabel.get().getIdentifiers().get(0));
            Assert.assertEquals("surname", personVertexLabel.get().getIdentifiers().get(1));
        }
    }

    @Test
    public void testEdgeCompositeIds() throws Exception {
        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("surname", PropertyType.varChar(100));
                    put("country", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("name", "surname"))
        );
        VertexLabel addressVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Address",
                new LinkedHashMap<>() {{
                    put("street", PropertyType.varChar(100));
                    put("suburb", PropertyType.varChar(100));
                    put("country", PropertyType.varChar(100));
                    put("province", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("street", "suburb"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.getTopology().ensureEdgeLabelExist(
                "livesAt",
                personVertexLabel,
                addressVertexLabel,
                new HashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2")));

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            this.sqlgGraph.tx().rollback();
            Optional<EdgeLabel> livesAt = this.sqlgGraph.getTopology().getEdgeLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "livesAt");
            Assert.assertFalse(livesAt.isPresent());
            if (this.sqlgGraph.getSqlDialect().supportsDistribution()) {
                livesAt = this.sqlgGraph1.getTopology().getEdgeLabel(this.sqlgGraph1.getSqlDialect().getPublicSchema(), "livesAt");
                Assert.assertFalse(livesAt.isPresent());
            }
        }

        this.sqlgGraph.getTopology().ensureEdgeLabelExist(
                "livesAt",
                personVertexLabel,
                addressVertexLabel,
                new HashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2")));
        this.sqlgGraph.tx().commit();

        Thread.sleep(1000);

        Optional<EdgeLabel> livesAt = this.sqlgGraph.getTopology().getEdgeLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "livesAt");
        Assert.assertTrue(livesAt.isPresent());
        Assert.assertEquals(2, livesAt.get().getIdentifiers().size());
        Assert.assertEquals("uid1", livesAt.get().getIdentifiers().get(0));
        Assert.assertEquals("uid2", livesAt.get().getIdentifiers().get(1));

        if (this.sqlgGraph.getSqlDialect().supportsDistribution()) {
            livesAt = this.sqlgGraph1.getTopology().getEdgeLabel(this.sqlgGraph1.getSqlDialect().getPublicSchema(), "livesAt");
            Assert.assertTrue(livesAt.isPresent());
            Assert.assertEquals(2, livesAt.get().getIdentifiers().size());
            Assert.assertEquals("uid1", livesAt.get().getIdentifiers().get(0));
            Assert.assertEquals("uid2", livesAt.get().getIdentifiers().get(1));
        }

        this.sqlgGraph.close();
        this.sqlgGraph = SqlgGraph.open(configuration);
        livesAt = this.sqlgGraph.getTopology().getEdgeLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "livesAt");
        Assert.assertTrue(livesAt.isPresent());
        Assert.assertEquals(2, livesAt.get().getIdentifiers().size());
        Assert.assertEquals("uid1", livesAt.get().getIdentifiers().get(0));
        Assert.assertEquals("uid2", livesAt.get().getIdentifiers().get(1));

        Thread.sleep(1000);

        dropSqlgSchema(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        this.sqlgGraph = SqlgGraph.open(configuration);
        livesAt = this.sqlgGraph.getTopology().getEdgeLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "livesAt");
        Assert.assertTrue(livesAt.isPresent());

        List<Vertex> edgeLabels =sqlgGraph.topology()
                .V().hasLabel(SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA).has(Topology.SQLG_SCHEMA_SCHEMA_NAME, this.sqlgGraph.getSqlDialect().getPublicSchema())
                .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                .out(SQLG_SCHEMA_OUT_EDGES_EDGE)
                .has(Topology.SQLG_SCHEMA_EDGE_LABEL_NAME, "livesAt")
                .toList();
        Preconditions.checkState(edgeLabels.size() == 1, "BUG: There can only ever be one EdgeLabel vertex, found %s", edgeLabels.size());

        Assert.assertEquals(2, livesAt.get().getIdentifiers().size());
        Assert.assertEquals("uid1", livesAt.get().getIdentifiers().get(0));
        Assert.assertEquals("uid2", livesAt.get().getIdentifiers().get(1));
    }

    @Test
    public void testEdgeNormal() throws Exception {
        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("surname", PropertyType.varChar(100));
                    put("country", PropertyType.varChar(100));
                }});
        VertexLabel addressVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Address",
                new LinkedHashMap<>() {{
                    put("street", PropertyType.varChar(100));
                    put("suburb", PropertyType.varChar(100));
                    put("country", PropertyType.varChar(100));
                    put("province", PropertyType.varChar(100));
                }});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.getTopology().ensureEdgeLabelExist(
                "livesAt",
                personVertexLabel,
                addressVertexLabel,
                new HashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }});
        this.sqlgGraph.tx().rollback();

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            Optional<EdgeLabel> livesAt = this.sqlgGraph.getTopology().getEdgeLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "livesAt");
            Assert.assertFalse(livesAt.isPresent());
        }
        if (this.sqlgGraph.getSqlDialect().supportsDistribution()) {
            Optional<EdgeLabel> livesAt = this.sqlgGraph1.getTopology().getEdgeLabel(this.sqlgGraph1.getSqlDialect().getPublicSchema(), "livesAt");
            Assert.assertFalse(livesAt.isPresent());
        }

        this.sqlgGraph.getTopology().ensureEdgeLabelExist(
                "livesAt",
                personVertexLabel,
                addressVertexLabel,
                new HashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }});
        this.sqlgGraph.tx().commit();

        Thread.sleep(1000);

        Optional<EdgeLabel> livesAt = this.sqlgGraph.getTopology().getEdgeLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "livesAt");
        Assert.assertTrue(livesAt.isPresent());

        if (this.sqlgGraph.getSqlDialect().supportsDistribution()) {
            livesAt = this.sqlgGraph1.getTopology().getEdgeLabel(this.sqlgGraph1.getSqlDialect().getPublicSchema(), "livesAt");
            Assert.assertTrue(livesAt.isPresent());
        }

        this.sqlgGraph.close();
        this.sqlgGraph = SqlgGraph.open(configuration);
        livesAt = this.sqlgGraph.getTopology().getEdgeLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "livesAt");
        Assert.assertTrue(livesAt.isPresent());

        Thread.sleep(1000);

        dropSqlgSchema(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        this.sqlgGraph = SqlgGraph.open(configuration);
        livesAt = this.sqlgGraph.getTopology().getEdgeLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "livesAt");
        Assert.assertTrue(livesAt.isPresent());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testDuplicatePath() {
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        vertexLabel.ensureEdgeLabelExist(
                "aa",
                vertexLabel,
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(List.of("uid")));
        this.sqlgGraph.tx().commit();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "a1", "age", 1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "a2", "age", 5);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "a3", "age", 7);
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "a4", "age", 5);
        a1.addEdge("aa", a2, "uid", UUID.randomUUID().toString());
        a1.addEdge("aa", a3, "uid", UUID.randomUUID().toString());
        a1.addEdge("aa", a4, "uid", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Integer> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V(a1).out("aa").values("age");
        String sql = getSQL(traversal);
        List<Integer> results = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertEquals(3, results.size(), 0);
        Assert.assertTrue(results.remove(Integer.valueOf(5)));
        Assert.assertTrue(results.remove(Integer.valueOf(7)));
        Assert.assertTrue(results.remove(Integer.valueOf(5)));

    }
}
