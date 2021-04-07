package org.umlg.sqlg.test.reducing;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.step.SqlgGraphStep;
import org.umlg.sqlg.step.SqlgGroupStep;
import org.umlg.sqlg.step.SqlgPropertiesStep;
import org.umlg.sqlg.step.barrier.*;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/11/17
 */
@SuppressWarnings({"Duplicates", "unchecked", "rawtypes"})
public class TestReducingUserSuppliedIds extends BaseTest {

    @Before
    public void before() throws Exception {
        super.before();
        if (isHsqldb()) {
            Connection connection = this.sqlgGraph.tx().getConnection();
            Statement statement = connection.createStatement();
            statement.execute("SET DATABASE SQL AVG SCALE 2");
            this.sqlgGraph.tx().commit();
        }
    }

    @Test
    public void testGroupCountNoBySomething() {
        for (String s : List.of("A", "B", "C")) {
            this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                    s,
                    new LinkedHashMap<>() {{
                        put("uid", PropertyType.varChar(100));
                        put("name", PropertyType.varChar(100));
                        put("age", PropertyType.INTEGER);
                    }},
                    ListOrderedSet.listOrderedSet(List.of("uid"))
            );
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "a", "age", 1);
        this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "a", "age", 2);
        this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "b", "age", 3);
        this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "name", "b", "age", 3);
        this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "name", "b", "age", 3);
        this.sqlgGraph.addVertex(T.label, "C", "uid", UUID.randomUUID().toString(), "name", "b", "age", 3);
        this.sqlgGraph.tx().commit();
        DefaultTraversal<Vertex, Map<Long, Long>> traversal = (DefaultTraversal<Vertex, Map<Long, Long>>) this.sqlgGraph.traversal().V().<Long>groupCount().by(__.bothE().count());
        printTraversalForm(traversal);
        List<Map<Long, Long>> result = traversal.toList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(1, result.get(0).size());
        Assert.assertTrue(result.get(0).values().stream().allMatch(a -> a == 6L));
    }

    @Test
    public void testGroupCountNoBy() {
        for (String s : List.of("A", "B", "C")) {
            this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                    s,
                    new LinkedHashMap<>() {{
                        put("uid", PropertyType.varChar(100));
                        put("name", PropertyType.varChar(100));
                        put("age", PropertyType.INTEGER);
                    }},
                    ListOrderedSet.listOrderedSet(List.of("uid"))
            );
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "a", "age", 1);
        this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "a", "age", 2);
        this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "b", "age", 3);
        this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "name", "b", "age", 3);
        this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "name", "b", "age", 3);
        this.sqlgGraph.addVertex(T.label, "C", "uid", UUID.randomUUID().toString(), "name", "b", "age", 3);
        this.sqlgGraph.tx().commit();
        DefaultTraversal<Vertex, Map<Object, Long>> traversal = (DefaultTraversal<Vertex, Map<Object, Long>>) sqlgGraph.traversal().V()
                .groupCount();
        printTraversalForm(traversal);
        List<Map<Object, Long>> result = traversal.toList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(6, result.get(0).size());
        Assert.assertTrue(result.get(0).values().stream().allMatch(a -> a == 1L));
    }

    @Test
    public void testGroupCount() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "a", "age", 1);
        this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "a", "age", 2);
        this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "b", "age", 3);
        this.sqlgGraph.tx().commit();
        DefaultTraversal<Vertex, Map<Object, Long>> traversal = (DefaultTraversal<Vertex, Map<Object, Long>>) this.sqlgGraph.traversal().V().hasLabel("A").groupCount().by("name");
        printTraversalForm(traversal);
        List<Map<Object, Long>> result = traversal.toList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(2, result.get(0).get("a"), 0);
        Assert.assertEquals(1, result.get(0).get("b"), 0);
    }

    @Test
    public void testGroupCountByLabel() {
        for (String s : List.of("A", "B", "C")) {
            this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                    s,
                    new LinkedHashMap<>() {{
                        put("uid1", PropertyType.varChar(100));
                        put("uid2", PropertyType.varChar(100));
                        put("name", PropertyType.varChar(100));
                        put("age", PropertyType.INTEGER);
                    }},
                    ListOrderedSet.listOrderedSet(List.of("uid1", "uid2"))
            );
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "a", "age", 1);
        this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "a", "age", 2);
        this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "b", "age", 3);
        this.sqlgGraph.addVertex(T.label, "B", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "b", "age", 3);
        this.sqlgGraph.addVertex(T.label, "B", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "b", "age", 3);
        this.sqlgGraph.addVertex(T.label, "C", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "b", "age", 3);
        this.sqlgGraph.tx().commit();
        DefaultTraversal<Vertex, Map<Object, Long>> traversal = (DefaultTraversal<Vertex, Map<Object, Long>>) this.sqlgGraph.traversal().V().groupCount().by(T.label);
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\tCOUNT(1) AS \"count\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_C\"", sql);
        }
        List<Map<Object, Long>> result = traversal.toList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(3, result.get(0).size());
        Assert.assertTrue(result.get(0).containsKey("A"));
        Assert.assertTrue(result.get(0).containsKey("B"));
        Assert.assertTrue(result.get(0).containsKey("C"));
        Assert.assertEquals(3L, result.get(0).get("A"), 0);
        Assert.assertEquals(2L, result.get(0).get("B"), 0);
        Assert.assertEquals(1L, result.get(0).get("C"), 0);
    }

    @Test
    public void testGroupByCountLabel() {
        for (String s : List.of("A", "B", "C")) {
            this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                    s,
                    new LinkedHashMap<>() {{
                        put("uid1", PropertyType.varChar(100));
                        put("uid2", PropertyType.varChar(100));
                        put("name", PropertyType.varChar(100));
                        put("age", PropertyType.INTEGER);
                    }},
                    ListOrderedSet.listOrderedSet(List.of("uid1", "uid2"))
            );
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "a", "age", 1);
        this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "a", "age", 2);
        this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "b", "age", 3);
        this.sqlgGraph.addVertex(T.label, "B", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "b", "age", 3);
        this.sqlgGraph.addVertex(T.label, "B", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "b", "age", 3);
        this.sqlgGraph.addVertex(T.label, "C", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "b", "age", 3);
        this.sqlgGraph.tx().commit();
        DefaultTraversal<Vertex, Map<Object, Long>> traversal = (DefaultTraversal<Vertex, Map<Object, Long>>) this.sqlgGraph.traversal().V()
                .<Object, Long>group().by(T.label).by(__.count());
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\tCOUNT(1) AS \"count\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_C\"", sql);
        }
        List<Map<Object, Long>> result = traversal.toList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(3, result.get(0).size());
        Assert.assertTrue(result.get(0).containsKey("A"));
        Assert.assertTrue(result.get(0).containsKey("B"));
        Assert.assertTrue(result.get(0).containsKey("C"));
        Assert.assertEquals(3L, result.get(0).get("A"), 0);
        Assert.assertEquals(2L, result.get(0).get("B"), 0);
        Assert.assertEquals(1L, result.get(0).get("C"), 0);
    }

    @Test
    public void testGroupByCount() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid1", "uid2"))
        );
        this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "a", "age", 1);
        this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "a", "age", 2);
        this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "b", "age", 3);
        this.sqlgGraph.tx().commit();
        DefaultTraversal<Vertex, Map<Object, Long>> traversal = (DefaultTraversal<Vertex, Map<Object, Long>>) this.sqlgGraph.traversal().V().hasLabel("A")
                .<Object, Long>group().by("name").by(__.count());
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\tCOUNT(1) AS \"count\",\n" +
                    "\t\"public\".\"V_A\".\"name\" AS \"alias1\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\"\n" +
                    "GROUP BY\n" +
                    "\t\"public\".\"V_A\".\"name\"", sql);
        }
        List<Map<Object, Long>> result = traversal.toList();
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.get(0).containsKey("a"));
        Assert.assertTrue(result.get(0).containsKey("b"));
        Assert.assertEquals(2L, result.get(0).get("a"), 0);
        Assert.assertEquals(1L, result.get(0).get("b"), 0);
    }

    @Test
    public void testMax() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                    put("x", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("name"))
        );
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a1", "age", 1, "x", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a2", "age", 2, "x", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a3", "age", 3, "x", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a4", "age", 0, "x", 1);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Integer> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V().hasLabel("Person").values("age").max();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\tMAX(\"public\".\"V_Person\".\"age\") AS \"alias1\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_Person\"", sql);
        }
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgMaxGlobalStep);
        Assert.assertEquals(3, traversal.next(), 0);
    }

    @Test
    public void testGroupOverOnePropertyMax() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "A", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "B", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "A", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "B", "age", 4);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Map<String, Integer>> traversal = (DefaultTraversal) sqlgGraph.traversal()
                .V().hasLabel("Person")
                .<String, Integer>group().by("name").by(__.values("age").max());
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_Person\".\"name\" AS \"alias1\",\n" +
                    "\tMAX(\"public\".\"V_Person\".\"age\") AS \"alias2\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_Person\"\n" +
                    "GROUP BY\n" +
                    "\t\"public\".\"V_Person\".\"name\"", sql);
        }
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgGroupStep);
        Map<String, Integer> result = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertTrue(result.containsKey("A"));
        Assert.assertTrue(result.containsKey("B"));
        Assert.assertEquals(3, result.get("A"), 0);
        Assert.assertEquals(4, result.get("B"), 0);
    }

    @Test
    public void testMaxOnString() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(List.of("name"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a1", "age", "a");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a2", "age", "b");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a3", "age", "d");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a4", "age", "c");
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, String> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V().hasLabel("Person").values("age").max();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\tMAX(\"public\".\"V_Person\".\"age\") AS \"alias1\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_Person\"", sql);
        }
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgMaxGlobalStep);
        Assert.assertEquals("d", traversal.next());
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void testMin() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("name"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a1", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a2", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a3", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a4", "age", 0);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Integer> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V().hasLabel("Person").values("age").min();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\tMIN(\"public\".\"V_Person\".\"age\") AS \"alias1\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_Person\"", sql);
        }
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgMinGlobalStep);
        Assert.assertEquals(0, traversal.next(), 0);
    }

    @Test
    public void testMinOnString() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(List.of("name"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a1", "age", "a");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a2", "age", "b");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a3", "age", "c");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a4", "age", "d");
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, String> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V().hasLabel("Person").values("age").min();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\tMIN(\"public\".\"V_Person\".\"age\") AS \"alias1\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_Person\"", sql);
        }
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgMinGlobalStep);
        Assert.assertEquals("a", traversal.next());
    }

    @Test
    public void testSum() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("name"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a1", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a2", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a3", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a4", "age", 0);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Long> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V().hasLabel("Person").values("age").sum();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\tSUM(\"public\".\"V_Person\".\"age\") AS \"alias1\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_Person\"", sql);
        }
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgSumGlobalStep);
        Assert.assertEquals(6, traversal.next(), 0L);
    }

    @Test
    public void testMean() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("name"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a1", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a2", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a3", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a4", "age", 0);
        this.sqlgGraph.tx().commit();
        DefaultTraversal<Vertex, Double> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V().hasLabel("Person").values("age").mean();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\tAVG(\"public\".\"V_Person\".\"age\") AS \"alias1\", COUNT(1) AS \"alias1_weight\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_Person\"", sql);
        }
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgAvgGlobalStep);
        Double d = traversal.next();
        Assert.assertEquals(1.5, d, 0D);
    }


    @Test
    public void testGroupOverOnePropertyMin() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "A", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "B", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "A", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "B", "age", 4);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Map<String, Integer>> traversal = (DefaultTraversal) sqlgGraph.traversal()
                .V().hasLabel("Person")
                .<String, Integer>group().by("name").by(__.values("age").min());
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_Person\".\"name\" AS \"alias1\",\n" +
                    "\tMIN(\"public\".\"V_Person\".\"age\") AS \"alias2\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_Person\"\n" +
                    "GROUP BY\n" +
                    "\t\"public\".\"V_Person\".\"name\"", sql);
        }
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgGroupStep);
        Map<String, Integer> result = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertTrue(result.containsKey("A"));
        Assert.assertTrue(result.containsKey("B"));
        Assert.assertEquals(1, result.get("A"), 0);
        Assert.assertEquals(2, result.get("B"), 0);
    }

    @Test
    public void testGroupOverOnePropertySum() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "A", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "B", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "A", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "B", "age", 4);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Map<String, Long>> traversal = (DefaultTraversal) sqlgGraph.traversal()
                .V().hasLabel("Person")
                .<String, Long>group().by("name").by(__.values("age").sum());
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_Person\".\"name\" AS \"alias1\",\n" +
                    "\tSUM(\"public\".\"V_Person\".\"age\") AS \"alias2\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_Person\"\n" +
                    "GROUP BY\n" +
                    "\t\"public\".\"V_Person\".\"name\"", sql);
        }
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgGroupStep);
        Map<String, Long> result = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertTrue(result.containsKey("A"));
        Assert.assertTrue(result.containsKey("B"));
        Assert.assertEquals(4, result.get("A"), 0L);
        Assert.assertEquals(6, result.get("B"), 0L);
    }

    @Test
    public void testGroupOverOnePropertyMean() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "A", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "B", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "A", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "B", "age", 4);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Map<String, Double>> traversal = (DefaultTraversal) sqlgGraph.traversal()
                .V().hasLabel("Person")
                .<String, Double>group().by("name").by(__.values("age").mean());
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_Person\".\"name\" AS \"alias1\",\n" +
                    "\tAVG(\"public\".\"V_Person\".\"age\") AS \"alias2\", COUNT(1) AS \"alias2_weight\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_Person\"\n" +
                    "GROUP BY\n" +
                    "\t\"public\".\"V_Person\".\"name\"", sql);
        }
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgGroupStep);
        Map<String, Double> result = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertTrue(result.containsKey("A"));
        Assert.assertTrue(result.containsKey("B"));
        Assert.assertEquals(2.0, result.get("A"), 0D);
        Assert.assertEquals(3.0, result.get("B"), 0D);
    }

    @Test
    public void testGroupOverTwoPropertiesWithValues() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("surname", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid1", "uid2"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "Person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "A", "surname", "C", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "B", "surname", "D", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "A", "surname", "C", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "B", "surname", "E", "age", 4);
        this.sqlgGraph.addVertex(T.label, "Person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "C", "surname", "E", "age", 5);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Map<List<String>, Integer>> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V().hasLabel("Person")
                .<List<String>, Integer>group()
                .by(__.values("name", "surname").fold())
                .by(__.values("age").max());

        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_Person\".\"surname\" AS \"alias1\",\n" +
                    "\t\"public\".\"V_Person\".\"name\" AS \"alias2\",\n" +
                    "\tMAX(\"public\".\"V_Person\".\"age\") AS \"alias3\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_Person\"\n" +
                    "GROUP BY\n" +
                    "\t\"public\".\"V_Person\".\"name\",\n" +
                    "\t\"public\".\"V_Person\".\"surname\"", sql);
        }
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgGroupStep);

        Map<List<String>, Integer> result = traversal.next();
        System.out.println(result);

        Assert.assertTrue(result.containsKey(Arrays.asList("A", "C")) || result.containsKey(Arrays.asList("C", "A")));
        Assert.assertTrue(result.containsKey(Arrays.asList("B", "D")) || result.containsKey(Arrays.asList("D", "B")));
        Assert.assertTrue(result.containsKey(Arrays.asList("B", "E")) || result.containsKey(Arrays.asList("E", "B")));
        Assert.assertTrue(result.containsKey(Arrays.asList("C", "E")) || result.containsKey(Arrays.asList("E", "C")));
        Assert.assertEquals(4, result.size());
        Assert.assertFalse(traversal.hasNext());

        if (result.containsKey(Arrays.asList("A", "C"))) {
            Assert.assertEquals(3, result.get(Arrays.asList("A", "C")), 0);
        } else {
            Assert.assertEquals(3, result.get(Arrays.asList("C", "A")), 0);
        }
        if (result.containsKey(Arrays.asList("B", "D"))) {
            Assert.assertEquals(2, result.get(Arrays.asList("B", "D")), 0);
        } else {
            Assert.assertEquals(2, result.get(Arrays.asList("D", "B")), 0);
        }
        if (result.containsKey(Arrays.asList("B", "E"))) {
            Assert.assertEquals(4, result.get(Arrays.asList("B", "E")), 0);
        } else {
            Assert.assertEquals(4, result.get(Arrays.asList("E", "B")), 0);
        }
        if (result.containsKey(Arrays.asList("C", "E"))) {
            Assert.assertEquals(5, result.get(Arrays.asList("C", "E")), 0);
        } else {
            Assert.assertEquals(5, result.get(Arrays.asList("E", "C")), 0);
        }
    }

    @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
    @Test
    public void testGroupOverTwoPropertiesWithValueMap() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("surname", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid1", "uid2"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "Person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "A", "surname", "C", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "B", "surname", "D", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "A", "surname", "C", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "B", "surname", "E", "age", 4);
        this.sqlgGraph.addVertex(T.label, "Person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "C", "surname", "E", "age", 5);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Map<Map<String, List<String>>, Integer>> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V().hasLabel("Person")
                .<Map<String, List<String>>, Integer>group()
                .by(__.valueMap("name", "surname"))
                .by(__.values("age").max());

        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_Person\".\"surname\" AS \"alias1\",\n" +
                    "\t\"public\".\"V_Person\".\"name\" AS \"alias2\",\n" +
                    "\tMAX(\"public\".\"V_Person\".\"age\") AS \"alias3\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_Person\"\n" +
                    "GROUP BY\n" +
                    "\t\"public\".\"V_Person\".\"name\",\n" +
                    "\t\"public\".\"V_Person\".\"surname\"", sql);
        }
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgGroupStep);

        Map<Map<String, List<String>>, Integer> result = traversal.next();
        System.out.println(result);

        Assert.assertTrue(result.containsKey(new HashMap<String, List<String>>() {{
            put("surname", Arrays.asList("C"));
            put("name", Arrays.asList("A"));
        }}));
        Assert.assertTrue(result.containsKey(new HashMap<String, List<String>>() {{
            put("surname", Arrays.asList("D"));
            put("name", Arrays.asList("B"));
        }}));
        Assert.assertTrue(result.containsKey(new HashMap<String, List<String>>() {{
            put("surname", Arrays.asList("E"));
            put("name", Arrays.asList("B"));
        }}));
        Assert.assertTrue(result.containsKey(new HashMap<String, List<String>>() {{
            put("surname", Arrays.asList("E"));
            put("name", Arrays.asList("C"));
        }}));

        Assert.assertEquals(3, result.get(new HashMap<String, List<String>>() {{
            put("surname", Arrays.asList("C"));
            put("name", Arrays.asList("A"));
        }}), 0);
        Assert.assertEquals(2, result.get(new HashMap<String, List<String>>() {{
            put("surname", Arrays.asList("D"));
            put("name", Arrays.asList("B"));
        }}), 0);
        Assert.assertEquals(4, result.get(new HashMap<String, List<String>>() {{
            put("surname", Arrays.asList("E"));
            put("name", Arrays.asList("B"));
        }}), 0);
        Assert.assertEquals(5, result.get(new HashMap<String, List<String>>() {{
            put("surname", Arrays.asList("E"));
            put("name", Arrays.asList("C"));
        }}), 0);
    }

    @Test
    public void testGroupOverOnePropertyWithJoin() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(List.of("uid1", "uid2"))
        );
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("year", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid1", "uid2"))
        );
        this.sqlgGraph.tx().commit();
        Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "A");
        Vertex address1 = this.sqlgGraph.addVertex(T.label, "Address", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "A", "year", 2);
        Vertex address2 = this.sqlgGraph.addVertex(T.label, "Address", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "A", "year", 4);
        Vertex address3 = this.sqlgGraph.addVertex(T.label, "Address", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "C", "year", 6);
        Vertex address4 = this.sqlgGraph.addVertex(T.label, "Address", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "D", "year", 8);
        Vertex address5 = this.sqlgGraph.addVertex(T.label, "Address", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "D", "year", 7);
        Vertex address6 = this.sqlgGraph.addVertex(T.label, "Address", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "D", "year", 6);
        person.addEdge("livesAt", address1);
        person.addEdge("livesAt", address2);
        person.addEdge("livesAt", address3);
        person.addEdge("livesAt", address4);
        person.addEdge("livesAt", address5);
        person.addEdge("livesAt", address6);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Map<String, Integer>> traversal = (DefaultTraversal) this.sqlgGraph.traversal()
                .V().hasLabel("Person")
                .out("livesAt")
                .<String, Integer>group()
                .by("name")
                .by(__.values("year").max());

        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\tMAX(\"public\".\"V_Address\".\"year\") AS \"alias1\",\n" +
                    "\t\"public\".\"V_Address\".\"name\" AS \"alias2\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_Person\" INNER JOIN\n" +
                    "\t\"public\".\"E_livesAt\" ON \"public\".\"V_Person\".\"uid1\" = \"public\".\"E_livesAt\".\"public.Person.uid1__O\" AND \"public\".\"V_Person\".\"uid2\" = \"public\".\"E_livesAt\".\"public.Person.uid2__O\" INNER JOIN\n" +
                    "\t\"public\".\"V_Address\" ON \"public\".\"E_livesAt\".\"public.Address__I\" = \"public\".\"V_Address\".\"ID\"\n" +
                    "GROUP BY\n" +
                    "\t\"public\".\"V_Address\".\"name\"", sql);
        }
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgGroupStep);

        Map<String, Integer> result = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertEquals(3, result.size());
        Assert.assertTrue(result.containsKey("A"));
        Assert.assertTrue(result.containsKey("C"));
        Assert.assertTrue(result.containsKey("D"));
        Assert.assertEquals(4, result.get("A"), 0);
        Assert.assertEquals(6, result.get("C"), 0);
        Assert.assertEquals(8, result.get("D"), 0);
    }

    @Test
    public void testGroupByLabelMax() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("name", "age"))
        );
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "Dog",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("name", "age"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "Person", "name", "A", "age", 10);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "B", "age", 20);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "C", "age", 100);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "D", "age", 40);

        this.sqlgGraph.addVertex(T.label, "Dog", "name", "A", "age", 10);
        this.sqlgGraph.addVertex(T.label, "Dog", "name", "B", "age", 200);
        this.sqlgGraph.addVertex(T.label, "Dog", "name", "C", "age", 30);
        this.sqlgGraph.addVertex(T.label, "Dog", "name", "D", "age", 40);

        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Map<String, Integer>> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V()
                .<String, Integer>group().by(T.label).by(__.values("age").max());
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\tMAX(\"public\".\"V_Dog\".\"age\") AS \"alias1\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_Dog\"", sql);
        }
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgGroupStep);

        Map<String, Integer> result = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.containsKey("Person"));
        Assert.assertTrue(result.containsKey("Dog"));
        Assert.assertEquals(100, result.get("Person"), 0);
        Assert.assertEquals(200, result.get("Dog"), 0);
    }

    @Test
    public void testDuplicatePathQuery() {
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

        DefaultTraversal<Vertex, Integer> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V(a1).out("aa").values("age").max();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("SELECT\n" +
                    "\tMAX(a2.\"alias2\") \n" +
                    "FROM (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"E_aa\".\"public.A.uid__I\" AS \"public.E_aa.public.A.uid__I\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_aa\" ON \"public\".\"V_A\".\"uid\" = \"public\".\"E_aa\".\"public.A.uid__O\"\n" +
                    "WHERE\n" +
                    "\t( \"public\".\"V_A\".\"uid\" = ?)\n" +
                    ") a1 INNER JOIN (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_A\".\"uid\" AS \"alias1\",\n" +
                    "\t\"public\".\"V_A\".\"age\" AS \"alias2\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\"\n" +
                    ") a2 ON a1.\"public.E_aa.public.A.uid__I\" = a2.\"alias1\"", sql);
        }
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgMaxGlobalStep);
        Assert.assertTrue(traversal.hasNext());
        Integer max = traversal.next();
        Assert.assertEquals(7, max, 0);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testDuplicatePathQueryAggregateOnUid() {
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("age"))
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
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "a2", "age", 2);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "a3", "age", 3);
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "a4", "age", 4);
        a1.addEdge("aa", a2, "uid", UUID.randomUUID().toString());
        a1.addEdge("aa", a3, "uid", UUID.randomUUID().toString());
        a1.addEdge("aa", a4, "uid", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Integer> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V(a1).out("aa").values("age").max();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("SELECT\n" +
                    "\tMAX(a2.\"alias2\") \n" +
                    "FROM (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"E_aa\".\"public.A.age__I\" AS \"public.E_aa.public.A.age__I\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_aa\" ON \"public\".\"V_A\".\"age\" = \"public\".\"E_aa\".\"public.A.age__O\"\n" +
                    "WHERE\n" +
                    "\t( \"public\".\"V_A\".\"age\" = ?)\n" +
                    ") a1 INNER JOIN (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_A\".\"age\" AS \"alias1\",\n" +
                    "\t\"public\".\"V_A\".\"age\" AS \"alias2\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\"\n" +
                    ") a2 ON a1.\"public.E_aa.public.A.age__I\" = a2.\"alias2\"", sql);
        }
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgMaxGlobalStep);
        Assert.assertTrue(traversal.hasNext());
        Integer max = traversal.next();
        Assert.assertEquals(4, max, 0);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testUserSuppliedIds() {
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("name"))
        );
        vertexLabel.ensureEdgeLabelExist("aa",
                vertexLabel,
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Set.of("uid")));
        this.sqlgGraph.tx().commit();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "age", 2);
        a1.addEdge("aa", a2, "uid", UUID.randomUUID().toString());
        a2.addEdge("aa", a1, "uid", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();
        List<Integer> ages = this.sqlgGraph.traversal().V(a1).out().out().<Integer>values("age").toList();
        Assert.assertEquals(1, ages.size());
        Assert.assertEquals(1, ages.get(0), 0);
    }

    @Test
    public void testDuplicatePathQuery2() {
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("name"))
        );
        vertexLabel.ensureEdgeLabelExist("aa",
                vertexLabel,
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Set.of("uid")));
        this.sqlgGraph.tx().commit();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "age", 5);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "age", 7);
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a4", "age", 5);
        a1.addEdge("aa", a2, "uid", UUID.randomUUID().toString());
        a1.addEdge("aa", a3, "uid", UUID.randomUUID().toString());
        a1.addEdge("aa", a4, "uid", UUID.randomUUID().toString());
        a2.addEdge("aa", a1, "uid", UUID.randomUUID().toString());
        a2.addEdge("aa", a3, "uid", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Integer> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V(a1).out("aa").out("aa").values("age").max();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("SELECT\n" +
                    "\tMAX(a3.\"alias3\") \n" +
                    "FROM (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"E_aa\".\"public.A.name__I\" AS \"public.E_aa.public.A.name__I\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_aa\" ON \"public\".\"V_A\".\"name\" = \"public\".\"E_aa\".\"public.A.name__O\"\n" +
                    "WHERE\n" +
                    "\t( \"public\".\"V_A\".\"name\" = ?)\n" +
                    ") a1 INNER JOIN (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_A\".\"name\" AS \"alias1\",\n" +
                    "\t\"public\".\"E_aa\".\"public.A.name__I\" AS \"public.E_aa.public.A.name__I\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_aa\" ON \"public\".\"V_A\".\"name\" = \"public\".\"E_aa\".\"public.A.name__O\"\n" +
                    ") a2 ON a1.\"public.E_aa.public.A.name__I\" = a2.\"alias1\" INNER JOIN (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_A\".\"name\" AS \"alias2\",\n" +
                    "\t\"public\".\"V_A\".\"age\" AS \"alias3\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\"\n" +
                    ") a3 ON a2.\"public.E_aa.public.A.name__I\" = a3.\"alias2\"", sql);
        }
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgMaxGlobalStep);
        Assert.assertTrue(traversal.hasNext());
        Integer max = traversal.next();
        Assert.assertEquals(7, max, 0);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testDuplicatePathQueryAggregateOnIdentifierMax() {
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("age"))
        );
        vertexLabel.ensureEdgeLabelExist("aa",
                vertexLabel,
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Set.of("uid")));
        this.sqlgGraph.tx().commit();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "age", 2);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "age", 3);
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a4", "age", 4);
        a1.addEdge("aa", a2, "uid", UUID.randomUUID().toString());
        a1.addEdge("aa", a3, "uid", UUID.randomUUID().toString());
        a1.addEdge("aa", a4, "uid", UUID.randomUUID().toString());
        a2.addEdge("aa", a1, "uid", UUID.randomUUID().toString());
        a2.addEdge("aa", a3, "uid", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Integer> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V(a1).out("aa").out("aa").values("age").max();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("SELECT\n" +
                    "\tMAX(a3.\"alias3\") \n" +
                    "FROM (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"E_aa\".\"public.A.age__I\" AS \"public.E_aa.public.A.age__I\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_aa\" ON \"public\".\"V_A\".\"age\" = \"public\".\"E_aa\".\"public.A.age__O\"\n" +
                    "WHERE\n" +
                    "\t( \"public\".\"V_A\".\"age\" = ?)\n" +
                    ") a1 INNER JOIN (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_A\".\"age\" AS \"alias1\",\n" +
                    "\t\"public\".\"E_aa\".\"public.A.age__I\" AS \"public.E_aa.public.A.age__I\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_aa\" ON \"public\".\"V_A\".\"age\" = \"public\".\"E_aa\".\"public.A.age__O\"\n" +
                    ") a2 ON a1.\"public.E_aa.public.A.age__I\" = a2.\"alias1\" INNER JOIN (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_A\".\"age\" AS \"alias2\",\n" +
                    "\t\"public\".\"V_A\".\"age\" AS \"alias3\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\"\n" +
                    ") a3 ON a2.\"public.E_aa.public.A.age__I\" = a3.\"alias3\"", sql);
        }
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgMaxGlobalStep);
        Assert.assertTrue(traversal.hasNext());
        Integer max = traversal.next();
        Assert.assertEquals(3, max, 0);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testDuplicatePathQueryAggregateOnIdentifierMean() {
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("age"))
        );
        vertexLabel.ensureEdgeLabelExist("aa",
                vertexLabel,
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Set.of("uid")));
        this.sqlgGraph.tx().commit();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "age", 2);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "age", 3);
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a4", "age", 4);
        a1.addEdge("aa", a2, "uid", UUID.randomUUID().toString());
        a1.addEdge("aa", a3, "uid", UUID.randomUUID().toString());
        a1.addEdge("aa", a4, "uid", UUID.randomUUID().toString());
        a2.addEdge("aa", a1, "uid", UUID.randomUUID().toString());
        a2.addEdge("aa", a3, "uid", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Double> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V(a1).out("aa").out("aa").values("age").mean();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("SELECT\n" +
                    "\tAVG(a3.\"alias3\"), COUNT(1) AS \"alias3_weight\" \n" +
                    "FROM (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"E_aa\".\"public.A.age__I\" AS \"public.E_aa.public.A.age__I\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_aa\" ON \"public\".\"V_A\".\"age\" = \"public\".\"E_aa\".\"public.A.age__O\"\n" +
                    "WHERE\n" +
                    "\t( \"public\".\"V_A\".\"age\" = ?)\n" +
                    ") a1 INNER JOIN (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_A\".\"age\" AS \"alias1\",\n" +
                    "\t\"public\".\"E_aa\".\"public.A.age__I\" AS \"public.E_aa.public.A.age__I\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_aa\" ON \"public\".\"V_A\".\"age\" = \"public\".\"E_aa\".\"public.A.age__O\"\n" +
                    ") a2 ON a1.\"public.E_aa.public.A.age__I\" = a2.\"alias1\" INNER JOIN (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_A\".\"age\" AS \"alias2\",\n" +
                    "\t\"public\".\"V_A\".\"age\" AS \"alias3\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\"\n" +
                    ") a3 ON a2.\"public.E_aa.public.A.age__I\" = a3.\"alias3\"", sql);
        }
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgAvgGlobalStep);
        Assert.assertTrue(traversal.hasNext());
        Double mean = traversal.next();
        Assert.assertEquals(2, mean, 0);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testDuplicatePathQueryAggregateOnIdentifierSum() {
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("age"))
        );
        vertexLabel.ensureEdgeLabelExist("aa",
                vertexLabel,
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Set.of("uid")));
        this.sqlgGraph.tx().commit();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "age", 2);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "age", 3);
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a4", "age", 4);
        a1.addEdge("aa", a2, "uid", UUID.randomUUID().toString());
        a1.addEdge("aa", a3, "uid", UUID.randomUUID().toString());
        a1.addEdge("aa", a4, "uid", UUID.randomUUID().toString());
        a2.addEdge("aa", a1, "uid", UUID.randomUUID().toString());
        a2.addEdge("aa", a3, "uid", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Long> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V(a1).out("aa").out("aa").values("age").sum();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("SELECT\n" +
                    "\tSUM(a3.\"alias3\") \n" +
                    "FROM (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"E_aa\".\"public.A.age__I\" AS \"public.E_aa.public.A.age__I\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_aa\" ON \"public\".\"V_A\".\"age\" = \"public\".\"E_aa\".\"public.A.age__O\"\n" +
                    "WHERE\n" +
                    "\t( \"public\".\"V_A\".\"age\" = ?)\n" +
                    ") a1 INNER JOIN (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_A\".\"age\" AS \"alias1\",\n" +
                    "\t\"public\".\"E_aa\".\"public.A.age__I\" AS \"public.E_aa.public.A.age__I\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_aa\" ON \"public\".\"V_A\".\"age\" = \"public\".\"E_aa\".\"public.A.age__O\"\n" +
                    ") a2 ON a1.\"public.E_aa.public.A.age__I\" = a2.\"alias1\" INNER JOIN (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_A\".\"age\" AS \"alias2\",\n" +
                    "\t\"public\".\"V_A\".\"age\" AS \"alias3\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\"\n" +
                    ") a3 ON a2.\"public.E_aa.public.A.age__I\" = a3.\"alias3\"", sql);
        }
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgSumGlobalStep);
        Assert.assertTrue(traversal.hasNext());
        Long sum = traversal.next();
        Assert.assertEquals(4, sum, 0);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testDuplicatePathQueryAggregateOnIdentifierMin() {
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("age"))
        );
        vertexLabel.ensureEdgeLabelExist("aa",
                vertexLabel,
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Set.of("uid")));
        this.sqlgGraph.tx().commit();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "age", 2);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "age", 3);
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a4", "age", 4);
        a1.addEdge("aa", a2, "uid", UUID.randomUUID().toString());
        a1.addEdge("aa", a3, "uid", UUID.randomUUID().toString());
        a1.addEdge("aa", a4, "uid", UUID.randomUUID().toString());
        a2.addEdge("aa", a1, "uid", UUID.randomUUID().toString());
        a2.addEdge("aa", a3, "uid", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Integer> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V(a1).out("aa").out("aa").values("age").min();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("SELECT\n" +
                    "\tMIN(a3.\"alias3\") \n" +
                    "FROM (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"E_aa\".\"public.A.age__I\" AS \"public.E_aa.public.A.age__I\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_aa\" ON \"public\".\"V_A\".\"age\" = \"public\".\"E_aa\".\"public.A.age__O\"\n" +
                    "WHERE\n" +
                    "\t( \"public\".\"V_A\".\"age\" = ?)\n" +
                    ") a1 INNER JOIN (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_A\".\"age\" AS \"alias1\",\n" +
                    "\t\"public\".\"E_aa\".\"public.A.age__I\" AS \"public.E_aa.public.A.age__I\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_aa\" ON \"public\".\"V_A\".\"age\" = \"public\".\"E_aa\".\"public.A.age__O\"\n" +
                    ") a2 ON a1.\"public.E_aa.public.A.age__I\" = a2.\"alias1\" INNER JOIN (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_A\".\"age\" AS \"alias2\",\n" +
                    "\t\"public\".\"V_A\".\"age\" AS \"alias3\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\"\n" +
                    ") a3 ON a2.\"public.E_aa.public.A.age__I\" = a3.\"alias3\"", sql);
        }
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgMinGlobalStep);
        Assert.assertTrue(traversal.hasNext());
        Integer min = traversal.next();
        Assert.assertEquals(1, min, 0);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testDuplicatePathQuery3() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("name"))
        );
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("name"))
        );
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "C",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("name"))
        );
        this.sqlgGraph.tx().commit();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1);
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1", "age", 1);
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2", "age", 2);
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3", "age", 3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1", "age", 1);
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2", "age", 2);
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3", "age", 3);
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        a1.addEdge("ac", c1);
        a1.addEdge("ac", c2);
        a1.addEdge("ac", c3);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Long> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V(a1).out("ab", "ac").values("age").sum();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\tSUM(\"public\".\"V_B\".\"age\") AS \"alias1\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_ab\" ON \"public\".\"V_A\".\"name\" = \"public\".\"E_ab\".\"public.A.name__O\" INNER JOIN\n" +
                    "\t\"public\".\"V_B\" ON \"public\".\"E_ab\".\"public.B.name__I\" = \"public\".\"V_B\".\"name\"\n" +
                    "WHERE\n" +
                    "\t( \"public\".\"V_A\".\"name\" = ?)", sql);
        }
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgSumGlobalStep);
        Assert.assertTrue(traversal.hasNext());
        Long max = traversal.next();
        Assert.assertEquals(12, max, 0);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testDuplicatePathGroupCountQuery2() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "C",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new LinkedHashMap<>() {{
            put("uid", PropertyType.varChar(100));
        }},
                ListOrderedSet.listOrderedSet(Set.of("uid")));
        this.sqlgGraph.tx().commit();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a", "age", 1, "uid", UUID.randomUUID().toString());
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 1, "uid", UUID.randomUUID().toString());
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 2, "uid", UUID.randomUUID().toString());
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 3, "uid", UUID.randomUUID().toString());
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 3, "uid", UUID.randomUUID().toString());
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "b", "age", 0, "uid", UUID.randomUUID().toString());
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "b", "age", 2, "uid", UUID.randomUUID().toString());
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "b", "age", 4, "uid", UUID.randomUUID().toString());
        a1.addEdge("ab", b1, "uid", UUID.randomUUID().toString());
        a1.addEdge("ab", b2, "uid", UUID.randomUUID().toString());
        a1.addEdge("ab", b3, "uid", UUID.randomUUID().toString());
        a1.addEdge("ab", b4, "uid", UUID.randomUUID().toString());
        a1.addEdge("ac", c1, "uid", UUID.randomUUID().toString());
        a1.addEdge("ac", c2, "uid", UUID.randomUUID().toString());
        a1.addEdge("ac", c3, "uid", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Map<String, Long>> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V(a1).out("ab", "ac").group().by("name").by(__.values("age").count());
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_B\".\"uid\" AS \"alias1\",\n" +
                    "\t\"public\".\"V_B\".\"name\" AS \"alias2\",\n" +
                    "\t\"public\".\"V_B\".\"age\" AS \"alias3\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_ab\" ON \"public\".\"V_A\".\"uid\" = \"public\".\"E_ab\".\"public.A.uid__O\" INNER JOIN\n" +
                    "\t\"public\".\"V_B\" ON \"public\".\"E_ab\".\"public.B.uid__I\" = \"public\".\"V_B\".\"uid\"\n" +
                    "WHERE\n" +
                    "\t( \"public\".\"V_A\".\"uid\" = ?)", sql);
        }
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof GroupStep);
        Map<String, Long> result = traversal.next();
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.containsKey("b"));
        Long l = result.get("b");
        Assert.assertEquals(7L, l, 0L);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testDuplicatePathGroupCountQuery() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        VertexLabel cVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "C",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new LinkedHashMap<>() {{
            put("uid", PropertyType.varChar(100));
        }}, ListOrderedSet.listOrderedSet(Set.of("uid")));
        aVertexLabel.ensureEdgeLabelExist("ac", cVertexLabel, new LinkedHashMap<>() {{
            put("uid", PropertyType.varChar(100));
        }}, ListOrderedSet.listOrderedSet(Set.of("uid")));
        this.sqlgGraph.tx().commit();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1, "uid", UUID.randomUUID().toString());
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 1, "uid", UUID.randomUUID().toString());
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 2, "uid", UUID.randomUUID().toString());
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 3, "uid", UUID.randomUUID().toString());
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 3, "uid", UUID.randomUUID().toString());
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "b", "age", 1, "uid", UUID.randomUUID().toString());
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "b", "age", 2, "uid", UUID.randomUUID().toString());
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "b", "age", 3, "uid", UUID.randomUUID().toString());
        a1.addEdge("ab", b1, "uid", UUID.randomUUID().toString());
        a1.addEdge("ab", b2, "uid", UUID.randomUUID().toString());
        a1.addEdge("ab", b3, "uid", UUID.randomUUID().toString());
        a1.addEdge("ab", b4, "uid", UUID.randomUUID().toString());
        a1.addEdge("ac", c1, "uid", UUID.randomUUID().toString());
        a1.addEdge("ac", c2, "uid", UUID.randomUUID().toString());
        a1.addEdge("ac", c3, "uid", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Map<String, Long>> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V(a1).out("ab", "ac").group().by("name").by(__.count());
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\tCOUNT(1) AS \"count\",\n" +
                    "\t\"public\".\"V_B\".\"name\" AS \"alias1\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_ab\" ON \"public\".\"V_A\".\"uid\" = \"public\".\"E_ab\".\"public.A.uid__O\" INNER JOIN\n" +
                    "\t\"public\".\"V_B\" ON \"public\".\"E_ab\".\"public.B.uid__I\" = \"public\".\"V_B\".\"uid\"\n" +
                    "WHERE\n" +
                    "\t( \"public\".\"V_A\".\"uid\" = ?)\n" +
                    "GROUP BY\n" +
                    "\t\"public\".\"V_B\".\"name\"", sql);
        }
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgGroupStep);
        Map<String, Long> result = traversal.next();
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.containsKey("b"));
        Assert.assertEquals(7, result.get("b"), 0);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testDuplicatePathGroupSumQuery() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        VertexLabel cVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "C",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new LinkedHashMap<>() {{
            put("uid", PropertyType.varChar(100));
        }}, ListOrderedSet.listOrderedSet(Set.of("uid")));
        aVertexLabel.ensureEdgeLabelExist("ac", cVertexLabel, new LinkedHashMap<>() {{
            put("uid", PropertyType.varChar(100));
        }}, ListOrderedSet.listOrderedSet(Set.of("uid")));
        this.sqlgGraph.tx().commit();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1, "uid", UUID.randomUUID().toString());
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 1, "uid", UUID.randomUUID().toString());
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 2, "uid", UUID.randomUUID().toString());
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 3, "uid", UUID.randomUUID().toString());
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 3, "uid", UUID.randomUUID().toString());
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "b", "age", 1, "uid", UUID.randomUUID().toString());
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "b", "age", 2, "uid", UUID.randomUUID().toString());
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "b", "age", 3, "uid", UUID.randomUUID().toString());
        a1.addEdge("ab", b1, "uid", UUID.randomUUID().toString());
        a1.addEdge("ab", b2, "uid", UUID.randomUUID().toString());
        a1.addEdge("ab", b3, "uid", UUID.randomUUID().toString());
        a1.addEdge("ab", b4, "uid", UUID.randomUUID().toString());
        a1.addEdge("ac", c1, "uid", UUID.randomUUID().toString());
        a1.addEdge("ac", c2, "uid", UUID.randomUUID().toString());
        a1.addEdge("ac", c3, "uid", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Map<String, Long>> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V(a1).out("ab", "ac").group().by("name").by(__.values("age").sum());
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_B\".\"name\" AS \"alias1\",\n" +
                    "\tSUM(\"public\".\"V_B\".\"age\") AS \"alias2\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_ab\" ON \"public\".\"V_A\".\"uid\" = \"public\".\"E_ab\".\"public.A.uid__O\" INNER JOIN\n" +
                    "\t\"public\".\"V_B\" ON \"public\".\"E_ab\".\"public.B.uid__I\" = \"public\".\"V_B\".\"uid\"\n" +
                    "WHERE\n" +
                    "\t( \"public\".\"V_A\".\"uid\" = ?)\n" +
                    "GROUP BY\n" +
                    "\t\"public\".\"V_B\".\"name\"", sql);
        }
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgGroupStep);
        Map<String, Long> result = traversal.next();
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.containsKey("b"));
        Assert.assertEquals(15, result.get("b"), 0);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testDuplicatePathGroupMaxQuery() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        VertexLabel cVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "C",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new LinkedHashMap<>() {{
            put("uid", PropertyType.varChar(100));
        }}, ListOrderedSet.listOrderedSet(Set.of("uid")));
        aVertexLabel.ensureEdgeLabelExist("ac", cVertexLabel, new LinkedHashMap<>() {{
            put("uid", PropertyType.varChar(100));
        }}, ListOrderedSet.listOrderedSet(Set.of("uid")));
        this.sqlgGraph.tx().commit();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1, "uid", UUID.randomUUID().toString());
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 1, "uid", UUID.randomUUID().toString());
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 2, "uid", UUID.randomUUID().toString());
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 3, "uid", UUID.randomUUID().toString());
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 3, "uid", UUID.randomUUID().toString());
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "b", "age", 1, "uid", UUID.randomUUID().toString());
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "b", "age", 2, "uid", UUID.randomUUID().toString());
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "b", "age", 4, "uid", UUID.randomUUID().toString());
        a1.addEdge("ab", b1, "uid", UUID.randomUUID().toString());
        a1.addEdge("ab", b2, "uid", UUID.randomUUID().toString());
        a1.addEdge("ab", b3, "uid", UUID.randomUUID().toString());
        a1.addEdge("ab", b4, "uid", UUID.randomUUID().toString());
        a1.addEdge("ac", c1, "uid", UUID.randomUUID().toString());
        a1.addEdge("ac", c2, "uid", UUID.randomUUID().toString());
        a1.addEdge("ac", c3, "uid", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Map<String, Integer>> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V(a1).out("ab", "ac").group().by("name").by(__.values("age").max());
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_B\".\"name\" AS \"alias1\",\n" +
                    "\tMAX(\"public\".\"V_B\".\"age\") AS \"alias2\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_ab\" ON \"public\".\"V_A\".\"uid\" = \"public\".\"E_ab\".\"public.A.uid__O\" INNER JOIN\n" +
                    "\t\"public\".\"V_B\" ON \"public\".\"E_ab\".\"public.B.uid__I\" = \"public\".\"V_B\".\"uid\"\n" +
                    "WHERE\n" +
                    "\t( \"public\".\"V_A\".\"uid\" = ?)\n" +
                    "GROUP BY\n" +
                    "\t\"public\".\"V_B\".\"name\"", sql);
        }
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgGroupStep);
        Map<String, Integer> result = traversal.next();
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.containsKey("b"));
        Assert.assertEquals(4, result.get("b"), 0);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testDuplicatePathGroupMinQuery() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        VertexLabel cVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "C",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new LinkedHashMap<>() {{
            put("uid", PropertyType.varChar(100));
        }}, ListOrderedSet.listOrderedSet(Set.of("uid")));
        aVertexLabel.ensureEdgeLabelExist("ac", cVertexLabel, new LinkedHashMap<>() {{
            put("uid", PropertyType.varChar(100));
        }}, ListOrderedSet.listOrderedSet(Set.of("uid")));
        this.sqlgGraph.tx().commit();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1, "uid", UUID.randomUUID().toString());
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 1, "uid", UUID.randomUUID().toString());
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 2, "uid", UUID.randomUUID().toString());
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 3, "uid", UUID.randomUUID().toString());
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 3, "uid", UUID.randomUUID().toString());
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "b", "age", 0, "uid", UUID.randomUUID().toString());
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "b", "age", 2, "uid", UUID.randomUUID().toString());
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "b", "age", 4, "uid", UUID.randomUUID().toString());
        a1.addEdge("ab", b1, "uid", UUID.randomUUID().toString());
        a1.addEdge("ab", b2, "uid", UUID.randomUUID().toString());
        a1.addEdge("ab", b3, "uid", UUID.randomUUID().toString());
        a1.addEdge("ab", b4, "uid", UUID.randomUUID().toString());
        a1.addEdge("ac", c1, "uid", UUID.randomUUID().toString());
        a1.addEdge("ac", c2, "uid", UUID.randomUUID().toString());
        a1.addEdge("ac", c3, "uid", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Map<String, Integer>> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V(a1).out("ab", "ac").group().by("name").by(__.values("age").min());
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_B\".\"name\" AS \"alias1\",\n" +
                    "\tMIN(\"public\".\"V_B\".\"age\") AS \"alias2\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_ab\" ON \"public\".\"V_A\".\"uid\" = \"public\".\"E_ab\".\"public.A.uid__O\" INNER JOIN\n" +
                    "\t\"public\".\"V_B\" ON \"public\".\"E_ab\".\"public.B.uid__I\" = \"public\".\"V_B\".\"uid\"\n" +
                    "WHERE\n" +
                    "\t( \"public\".\"V_A\".\"uid\" = ?)\n" +
                    "GROUP BY\n" +
                    "\t\"public\".\"V_B\".\"name\"", sql);
        }
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgGroupStep);
        Map<String, Integer> result = traversal.next();
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.containsKey("b"));
        Assert.assertEquals(0, result.get("b"), 0);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testDuplicatePathGroupMeanQuery() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        VertexLabel cVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "C",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new LinkedHashMap<>() {{
            put("uid", PropertyType.varChar(100));
        }}, ListOrderedSet.listOrderedSet(Set.of("uid")));
        aVertexLabel.ensureEdgeLabelExist("ac", cVertexLabel, new LinkedHashMap<>() {{
            put("uid", PropertyType.varChar(100));
        }}, ListOrderedSet.listOrderedSet(Set.of("uid")));
        this.sqlgGraph.tx().commit();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1, "uid", UUID.randomUUID().toString());
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 1, "uid", UUID.randomUUID().toString());
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 2, "uid", UUID.randomUUID().toString());
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 3, "uid", UUID.randomUUID().toString());
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 3, "uid", UUID.randomUUID().toString());
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "b", "age", 0, "uid", UUID.randomUUID().toString());
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "b", "age", 2, "uid", UUID.randomUUID().toString());
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "b", "age", 4, "uid", UUID.randomUUID().toString());
        a1.addEdge("ab", b1, "uid", UUID.randomUUID().toString());
        a1.addEdge("ab", b2, "uid", UUID.randomUUID().toString());
        a1.addEdge("ab", b3, "uid", UUID.randomUUID().toString());
        a1.addEdge("ab", b4, "uid", UUID.randomUUID().toString());
        a1.addEdge("ac", c1, "uid", UUID.randomUUID().toString());
        a1.addEdge("ac", c2, "uid", UUID.randomUUID().toString());
        a1.addEdge("ac", c3, "uid", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Map<String, Double>> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V(a1).out("ab", "ac").group().by("name").by(__.values("age").mean());
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_B\".\"name\" AS \"alias1\",\n" +
                    "\tAVG(\"public\".\"V_B\".\"age\") AS \"alias2\", COUNT(1) AS \"alias2_weight\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_ab\" ON \"public\".\"V_A\".\"uid\" = \"public\".\"E_ab\".\"public.A.uid__O\" INNER JOIN\n" +
                    "\t\"public\".\"V_B\" ON \"public\".\"E_ab\".\"public.B.uid__I\" = \"public\".\"V_B\".\"uid\"\n" +
                    "WHERE\n" +
                    "\t( \"public\".\"V_A\".\"uid\" = ?)\n" +
                    "GROUP BY\n" +
                    "\t\"public\".\"V_B\".\"name\"", sql);
        }
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgGroupStep);
        Map<String, Double> result = traversal.next();
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.containsKey("b"));
        Double d = result.get("b");
        Assert.assertEquals(2.142857142857143D, d, 0D);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testGroupByDuplicatePaths() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        aVertexLabel.ensureEdgeLabelExist("aa", aVertexLabel, new LinkedHashMap<>() {{
            put("uid", PropertyType.varChar(100));
        }}, ListOrderedSet.listOrderedSet(Set.of("uid")));
        this.sqlgGraph.tx().commit();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1, "uid", UUID.randomUUID().toString());
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "age", 2, "uid", UUID.randomUUID().toString());
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "age", 3, "uid", UUID.randomUUID().toString());
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "age", 4, "uid", UUID.randomUUID().toString());
        Vertex a5 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "age", 5, "uid", UUID.randomUUID().toString());
        Vertex a6 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "age", 6, "uid", UUID.randomUUID().toString());
        a1.addEdge("aa", a2, "uid", UUID.randomUUID().toString());
        a1.addEdge("aa", a3, "uid", UUID.randomUUID().toString());
        a1.addEdge("aa", a4, "uid", UUID.randomUUID().toString());
        a1.addEdge("aa", a5, "uid", UUID.randomUUID().toString());
        a1.addEdge("aa", a6, "uid", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Map<String, Integer>> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V(a1).out().<String, Integer>group().by("name").by(__.values("age").max());
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("SELECT\n" +
                    "\ta2.\"alias2\", MAX(a2.\"alias3\") \n" +
                    "FROM (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"E_aa\".\"public.A.uid__I\" AS \"public.E_aa.public.A.uid__I\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_aa\" ON \"public\".\"V_A\".\"uid\" = \"public\".\"E_aa\".\"public.A.uid__O\"\n" +
                    "WHERE\n" +
                    "\t( \"public\".\"V_A\".\"uid\" = ?)\n" +
                    ") a1 INNER JOIN (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_A\".\"uid\" AS \"alias1\",\n" +
                    "\t\"public\".\"V_A\".\"name\" AS \"alias2\",\n" +
                    "\t\"public\".\"V_A\".\"age\" AS \"alias3\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\"\n" +
                    ") a2 ON a1.\"public.E_aa.public.A.uid__I\" = a2.\"alias1\"\n" +
                    "GROUP BY\n" +
                    "\ta2.\"alias2\"", sql);
        }
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgGroupStep);
        Assert.assertTrue(traversal.hasNext());
        Map<String, Integer> result = traversal.next();
        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.containsKey("a2"));
        Assert.assertEquals(3, result.get("a2"), 0);
        Assert.assertTrue(result.containsKey("a3"));
        Assert.assertEquals(6, result.get("a3"), 0);
    }

    @Test
    public void duplicateQueryMax() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("age"))
        );
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("age"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "A", "age", 1);
        this.sqlgGraph.addVertex(T.label, "B", "age", 2);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Integer> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V().values("age").max();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\tMAX(\"public\".\"V_B\".\"age\") AS \"alias1\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_B\"", sql);
        }
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgMaxGlobalStep);

        Assert.assertEquals(2, traversal.next(), 0);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void duplicateQueryMin() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("age"))
        );
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("age"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "A", "age", 1);
        this.sqlgGraph.addVertex(T.label, "B", "age", 2);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Integer> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V().values("age").min();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\tMIN(\"public\".\"V_B\".\"age\") AS \"alias1\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_B\"", sql);
        }
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgMinGlobalStep);

        Assert.assertEquals(1, traversal.next(), 0);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void duplicateQuerySum() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("age"))
        );
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("age"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "A", "age", 1);
        this.sqlgGraph.addVertex(T.label, "B", "age", 2);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Long> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V().values("age").sum();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\tSUM(\"public\".\"V_B\".\"age\") AS \"alias1\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_B\"", sql);
        }
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgSumGlobalStep);

        Assert.assertEquals(3, traversal.next(), 0L);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void duplicateQueryMean() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("name"))
        );
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("name"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "A", "age", 1, "name", "a1");
        this.sqlgGraph.addVertex(T.label, "B", "age", 2, "name", "b1");
        this.sqlgGraph.addVertex(T.label, "B", "age", 2, "name", "b2");
        this.sqlgGraph.addVertex(T.label, "B", "age", 2, "name", "b3");
        this.sqlgGraph.addVertex(T.label, "B", "age", 2, "name", "b4");
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Double> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V().values("age").mean();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\tAVG(\"public\".\"V_B\".\"age\") AS \"alias1\", COUNT(1) AS \"alias1_weight\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_B\"", sql);
        }
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgAvgGlobalStep);

        Assert.assertEquals(1.8, traversal.next(), 0);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testDuplicateQueryNoLabel() {
        VertexLabel person = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "person",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("name"))
        );
        person.ensureEdgeLabelExist("knows", person, new LinkedHashMap<>() {{
            put("uid", PropertyType.varChar(100));
        }}, ListOrderedSet.listOrderedSet(Set.of("uid")));
        this.sqlgGraph.tx().commit();
        Vertex p1 = this.sqlgGraph.addVertex(T.label, "person", "age", 1, "name", "person1");
        Vertex p2 = this.sqlgGraph.addVertex(T.label, "person", "age", 2, "name", "person2");
        p1.addEdge("knows", p2, "uid", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Double> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V().hasLabel("person").out().out().values("age").max();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("SELECT\n" +
                    "\tMAX(a3.\"alias3\") \n" +
                    "FROM (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"E_knows\".\"public.person.name__I\" AS \"public.E_knows.public.person.name__I\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_person\" INNER JOIN\n" +
                    "\t\"public\".\"E_knows\" ON \"public\".\"V_person\".\"name\" = \"public\".\"E_knows\".\"public.person.name__O\"\n" +
                    ") a1 INNER JOIN (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_person\".\"name\" AS \"alias1\",\n" +
                    "\t\"public\".\"E_knows\".\"public.person.name__I\" AS \"public.E_knows.public.person.name__I\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_person\" INNER JOIN\n" +
                    "\t\"public\".\"E_knows\" ON \"public\".\"V_person\".\"name\" = \"public\".\"E_knows\".\"public.person.name__O\"\n" +
                    ") a2 ON a1.\"public.E_knows.public.person.name__I\" = a2.\"alias1\" INNER JOIN (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_person\".\"name\" AS \"alias2\",\n" +
                    "\t\"public\".\"V_person\".\"age\" AS \"alias3\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_person\"\n" +
                    ") a3 ON a2.\"public.E_knows.public.person.name__I\" = a3.\"alias2\"", sql);
        }
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgMaxGlobalStep);
        Assert.assertTrue(traversal.hasNext());
        Number m = traversal.next();
        //noinspection ConstantConditions
        Assert.assertTrue(m instanceof Double);
        //noinspection CastCanBeRemovedNarrowingVariableType
        Assert.assertEquals(Double.NaN, (Double) m, 0D);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testMaxAgain() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("name"))
        );
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "Dog",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("name"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "Person", "age", 1, "name", "person1");
        this.sqlgGraph.addVertex(T.label, "Person", "age", 3, "name", "person2");
        this.sqlgGraph.addVertex(T.label, "Person", "age", 5, "name", "person3");
        this.sqlgGraph.addVertex(T.label, "Dog", "age", 7, "name", "dog1");
        this.sqlgGraph.tx().commit();
        Traversal<Vertex, Integer> traversal = this.sqlgGraph.traversal().V().values("age").max();
        printTraversalForm(traversal);
        Integer max = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertEquals(7, max, 0);
    }

    @Test
    public void testSummingNothing() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("name"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1);
        this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "age", 1);
        this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "age", 1);
        this.sqlgGraph.addVertex(T.label, "A", "name", "a4", "age", 1);
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Map<String, Number>> traversal = this.sqlgGraph.traversal()
                .V().has("name", "a5")
                .<String, Number>group().by(T.label).by(__.values("age").sum());
        printTraversalForm(traversal);
        Assert.assertTrue(traversal.hasNext());
        Map<String, Number> result = traversal.next();
        Assert.assertTrue(result.containsKey("A"));
        Assert.assertEquals(Double.NaN, (Double) result.get("A"), 0D);
    }

    @Test
    public void testCount() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("uid", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "A", "name", "a", "uid", 1);
        this.sqlgGraph.addVertex(T.label, "A", "name", "a", "uid", 2);
        this.sqlgGraph.addVertex(T.label, "A", "name", "a", "uid", 3);
        this.sqlgGraph.addVertex(T.label, "A", "name", "a", "uid", 4);
        this.sqlgGraph.tx().commit();
        DefaultTraversal<Vertex, Long> traversal = (DefaultTraversal<Vertex, Long>) this.sqlgGraph.traversal().V().count();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\tCOUNT(1) AS \"count\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\"", sql);
        }
        Assert.assertEquals(4, traversal.next(), 0);
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgCountGlobalStep);
    }

    @Test
    public void testCountStartEdge() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(List.of("name"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(List.of("name"))
        );
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new LinkedHashMap<>() {{
            put("name", PropertyType.varChar(100));
        }}, ListOrderedSet.listOrderedSet(Set.of("name")));
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
        a1.addEdge("ab", b1, "name", "edge1");
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").outE().otherV().toList();
        Assert.assertEquals(1, vertices.size());
        DefaultTraversal<Vertex, Long> traversal = (DefaultTraversal<Vertex, Long>) this.sqlgGraph.traversal().V().hasLabel("A").outE().otherV().count();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\tCOUNT(1) AS \"count\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_ab\" ON \"public\".\"V_A\".\"name\" = \"public\".\"E_ab\".\"public.A.name__O\" INNER JOIN\n" +
                    "\t\"public\".\"V_B\" ON \"public\".\"E_ab\".\"public.B.name__I\" = \"public\".\"V_B\".\"name\"", sql);
        }
        Assert.assertEquals(1, traversal.next(), 0);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testCountWithJoins() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(List.of("name"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(List.of("uid"))
        );
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new LinkedHashMap<>() {{
            put("name", PropertyType.varChar(100));
        }}, ListOrderedSet.listOrderedSet(Set.of("name")));
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a", "age", 1);
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 2, "uid", UUID.randomUUID().toString());
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 2, "uid", UUID.randomUUID().toString());
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 5, "uid", UUID.randomUUID().toString());
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "b", "age", 2, "uid", UUID.randomUUID().toString());
        a1.addEdge("ab", b1, "name", "edge1");
        a1.addEdge("ab", b2, "name", "edge2");
        a1.addEdge("ab", b3, "name", "edge3");
        a1.addEdge("ab", b4, "name", "edge4");
        this.sqlgGraph.tx().commit();
        DefaultTraversal<Vertex, Long> traversal = (DefaultTraversal<Vertex, Long>) this.sqlgGraph.traversal().V().hasLabel("A").out("ab").count();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\tCOUNT(1) AS \"count\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_ab\" ON \"public\".\"V_A\".\"name\" = \"public\".\"E_ab\".\"public.A.name__O\" INNER JOIN\n" +
                    "\t\"public\".\"V_B\" ON \"public\".\"E_ab\".\"public.B.uid__I\" = \"public\".\"V_B\".\"uid\"", sql);
        }
        Assert.assertEquals(4, traversal.next(), 0);
        Assert.assertFalse(traversal.hasNext());
        Traversal<Vertex, Integer> maxTraversal  = this.sqlgGraph.traversal().V().outE().otherV().values("age").max();
        sql = getSQL(maxTraversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\tMAX(\"public\".\"V_B\".\"age\") AS \"alias1\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_ab\" ON \"public\".\"V_A\".\"name\" = \"public\".\"E_ab\".\"public.A.name__O\" INNER JOIN\n" +
                    "\t\"public\".\"V_B\" ON \"public\".\"E_ab\".\"public.B.uid__I\" = \"public\".\"V_B\".\"uid\"", sql);
        }
        Assert.assertEquals(5, maxTraversal.next(), 0);
        Assert.assertFalse(maxTraversal.hasNext());
        traversal = (DefaultTraversal<Vertex, Long>) this.sqlgGraph.traversal().V().outE().otherV().count();
        sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\tCOUNT(1) AS \"count\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_ab\" ON \"public\".\"V_A\".\"name\" = \"public\".\"E_ab\".\"public.A.name__O\" INNER JOIN\n" +
                    "\t\"public\".\"V_B\" ON \"public\".\"E_ab\".\"public.B.uid__I\" = \"public\".\"V_B\".\"uid\"", sql);
        }
        Assert.assertEquals(4, traversal.next(), 0);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testCountMultipleLabels() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("prop", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(List.of("prop"))
        );
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(List.of("name"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "A", "prop", "a1");
        this.sqlgGraph.addVertex(T.label, "A", "prop", "a2");
        this.sqlgGraph.addVertex(T.label, "A", "prop", "a3");
        this.sqlgGraph.addVertex(T.label, "A", "prop", "a4");
        this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        this.sqlgGraph.addVertex(T.label, "B", "name", "b4");
        this.sqlgGraph.tx().commit();
        DefaultTraversal<Vertex, Long> traversal = (DefaultTraversal<Vertex, Long>) this.sqlgGraph.traversal().V().count();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\tCOUNT(1) AS \"count\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_B\"", sql);
        }
        Assert.assertEquals(8, traversal.next(), 0);
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgCountGlobalStep);
    }

    @Test
    public void testCountDuplicatePaths() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(List.of("name"))
        );
        aVertexLabel.ensureEdgeLabelExist("aa", aVertexLabel, new LinkedHashMap<>() {{
            put("uid", PropertyType.varChar(100));
        }}, ListOrderedSet.listOrderedSet(Set.of("uid")));
        this.sqlgGraph.tx().commit();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a4");
        a1.addEdge("aa", a2, "uid", "uid1");
        a2.addEdge("aa", a3, "uid", "uid2");
        a2.addEdge("aa", a4, "uid", "uid3");
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Long> traversal = (DefaultTraversal<Vertex, Long>) this.sqlgGraph.traversal().V(a1).out().out().count();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("SELECT\n" +
                    "\tCOUNT(1) \n" +
                    "FROM (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"E_aa\".\"public.A.name__I\" AS \"public.E_aa.public.A.name__I\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_aa\" ON \"public\".\"V_A\".\"name\" = \"public\".\"E_aa\".\"public.A.name__O\"\n" +
                    "WHERE\n" +
                    "\t( \"public\".\"V_A\".\"name\" = ?)\n" +
                    ") a1 INNER JOIN (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_A\".\"name\" AS \"alias1\",\n" +
                    "\t\"public\".\"E_aa\".\"public.A.name__I\" AS \"public.E_aa.public.A.name__I\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_aa\" ON \"public\".\"V_A\".\"name\" = \"public\".\"E_aa\".\"public.A.name__O\"\n" +
                    ") a2 ON a1.\"public.E_aa.public.A.name__I\" = a2.\"alias1\" INNER JOIN (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_A\".\"name\" AS \"alias2\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\"\n" +
                    ") a3 ON a2.\"public.E_aa.public.A.name__I\" = a3.\"alias2\"", sql);
        }
        Assert.assertEquals(2, traversal.next(), 0);

        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgCountGlobalStep);
    }

    @Test
    public void testCountOnPropertyNotYetExists() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("prop", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(List.of("prop"))
        );
        this.sqlgGraph.addVertex(T.label, "Person", "prop", "what");
        this.sqlgGraph.tx().commit();
        DefaultTraversal<Vertex, Long> traversal = (DefaultTraversal<Vertex, Long>) this.sqlgGraph.traversal().V().has(T.label, "Person").has("name", "john").count();
        List<Long> count = traversal.toList();
        Assert.assertEquals(1, count.size());
        Assert.assertEquals(0L, count.get(0), 0);
    }

    @Test
    public void testMaxDuplicatePaths() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.varChar(100));
                    put("age", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("name"))
        );
        aVertexLabel.ensureEdgeLabelExist("aa", aVertexLabel, new LinkedHashMap<>() {{
            put("uid", PropertyType.varChar(100));
        }}, ListOrderedSet.listOrderedSet(Set.of("uid")));
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "age", 2);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "age", 3);
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a4", "age", 4);
        a1.addEdge("aa", a2, "uid", "uid1");
        a2.addEdge("aa", a3, "uid", "uid2");
        a2.addEdge("aa", a4, "uid", "uid3");
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Integer> traversal = this.sqlgGraph.traversal().V(a1).out().out().values("age").max();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("SELECT\n" +
                    "\tMAX(a3.\"alias3\") \n" +
                    "FROM (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"E_aa\".\"public.A.name__I\" AS \"public.E_aa.public.A.name__I\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_aa\" ON \"public\".\"V_A\".\"name\" = \"public\".\"E_aa\".\"public.A.name__O\"\n" +
                    "WHERE\n" +
                    "\t( \"public\".\"V_A\".\"name\" = ?)\n" +
                    ") a1 INNER JOIN (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_A\".\"name\" AS \"alias1\",\n" +
                    "\t\"public\".\"E_aa\".\"public.A.name__I\" AS \"public.E_aa.public.A.name__I\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\" INNER JOIN\n" +
                    "\t\"public\".\"E_aa\" ON \"public\".\"V_A\".\"name\" = \"public\".\"E_aa\".\"public.A.name__O\"\n" +
                    ") a2 ON a1.\"public.E_aa.public.A.name__I\" = a2.\"alias1\" INNER JOIN (\n" +
                    "SELECT\n" +
                    "\t\"public\".\"V_A\".\"name\" AS \"alias2\",\n" +
                    "\t\"public\".\"V_A\".\"age\" AS \"alias3\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_A\"\n" +
                    ") a3 ON a2.\"public.E_aa.public.A.name__I\" = a3.\"alias2\"", sql);
        }
        Assert.assertEquals(4, traversal.next(), 0);
    }

    @Test
    public void testUserSuppliedPKCount() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "Person",
                        new HashMap<>() {{
                            put("uid", PropertyType.varChar(100));
                            put("name", PropertyType.varChar(100));
                        }},
                        ListOrderedSet.listOrderedSet(Collections.singletonList("uid"))
                );
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "marko");
        this.sqlgGraph.tx().commit();
        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString());
        john.property("name", "john");
        this.sqlgGraph.tx().commit();
        Traversal<Vertex, Long> traversal = this.sqlgGraph.traversal().V().hasLabel("Person").count();
        String sql = getSQL(traversal);
        if (isPostgres()) {
            Assert.assertEquals("\n" +
                    "SELECT\n" +
                    "\tCOUNT(1) AS \"count\"\n" +
                    "FROM\n" +
                    "\t\"public\".\"V_Person\"", sql);
        }
        Assert.assertEquals(2, traversal.next().intValue());
        Assert.assertEquals("marko", this.sqlgGraph.traversal().V(marko).next().value("name"));
        Assert.assertEquals("john", this.sqlgGraph.traversal().V(john).next().value("name"));
    }

    /**
     * This test BaseStrategy skipping optimizing count which comes after a SqlgPropertiesStep
     */
    @Test
    public void testAndColumns() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "Person",
                        new HashMap<>() {{
                            put("name1", PropertyType.varChar(100));
                            put("name2", PropertyType.varChar(100));
                        }},
                        ListOrderedSet.listOrderedSet(Collections.singletonList("name1"))
                );
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name1", "marko");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, vertexTraversal(this.sqlgGraph, v1).properties().count().next(), 0);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name2", "john", "name1", "Jones");
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().count().next(), 0);
        Assert.assertEquals(v2, this.sqlgGraph.traversal().V(v2.id()).next());
        Assert.assertEquals(2, vertexTraversal(this.sqlgGraph, v2).properties().count().next(), 0);
    }

}
