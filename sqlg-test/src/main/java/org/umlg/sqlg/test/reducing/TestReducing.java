package org.umlg.sqlg.test.reducing;

import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.step.SqlgGraphStep;
import org.umlg.sqlg.step.SqlgGroupStep;
import org.umlg.sqlg.step.SqlgPropertiesStep;
import org.umlg.sqlg.step.barrier.SqlgAvgGlobalStep;
import org.umlg.sqlg.step.barrier.SqlgMaxGlobalStep;
import org.umlg.sqlg.step.barrier.SqlgMinGlobalStep;
import org.umlg.sqlg.step.barrier.SqlgSumGlobalStep;
import org.umlg.sqlg.test.BaseTest;

import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.junit.Assert.assertFalse;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/11/17
 */
@SuppressWarnings({"Duplicates", "unchecked", "rawtypes"})
public class TestReducing extends BaseTest {

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

    @SuppressWarnings("Duplicates")
    @Test
    public void testMax() {
        this.sqlgGraph.addVertex(T.label, "Person", "age", 1, "x", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 2, "x", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 3, "x", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 0, "x", 1);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Integer> traversal = (DefaultTraversal)this.sqlgGraph.traversal().V().hasLabel("Person").values("age").max();
        printTraversalForm(traversal);
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgMaxGlobalStep);
        Assert.assertEquals(3, traversal.next(), 0);
    }

    @Test
    public void testMaxOnString() {
        this.sqlgGraph.addVertex(T.label, "Person", "age", "a");
        this.sqlgGraph.addVertex(T.label, "Person", "age", "b");
        this.sqlgGraph.addVertex(T.label, "Person", "age", "d");
        this.sqlgGraph.addVertex(T.label, "Person", "age", "c");
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, String> traversal = (DefaultTraversal)this.sqlgGraph.traversal().V().hasLabel("Person").values("age").max();
        printTraversalForm(traversal);
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgMaxGlobalStep);
        Assert.assertEquals("d", traversal.next());
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void testMin() {
        this.sqlgGraph.addVertex(T.label, "Person", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 0);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Integer> traversal = (DefaultTraversal)this.sqlgGraph.traversal().V().hasLabel("Person").values("age").min();
        printTraversalForm(traversal);
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgMinGlobalStep);
        Assert.assertEquals(0, traversal.next(), 0);
    }

    @Test
    public void testMinOnString() {
        this.sqlgGraph.addVertex(T.label, "Person", "age", "a");
        this.sqlgGraph.addVertex(T.label, "Person", "age", "b");
        this.sqlgGraph.addVertex(T.label, "Person", "age", "c");
        this.sqlgGraph.addVertex(T.label, "Person", "age", "d");
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, String> traversal = (DefaultTraversal)this.sqlgGraph.traversal().V().hasLabel("Person").values("age").min();
        printTraversalForm(traversal);
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgMinGlobalStep);
        Assert.assertEquals("a", traversal.next());
    }

    @Test
    public void testSum() {
        this.sqlgGraph.addVertex(T.label, "Person", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 0);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Long> traversal = (DefaultTraversal)this.sqlgGraph.traversal().V().hasLabel("Person").values("age").sum();
        printTraversalForm(traversal);
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgSumGlobalStep);
        Assert.assertEquals(6, traversal.next(), 0L);
    }

    @Test
    public void testMean() {
        this.sqlgGraph.addVertex(T.label, "Person", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 0);
        this.sqlgGraph.tx().commit();
        DefaultTraversal<Vertex, Double> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V().hasLabel("Person").values("age").mean();
        printTraversalForm(traversal);
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgAvgGlobalStep);
        Double d = traversal.next();
        Assert.assertEquals(1.5, d, 0D);
    }

    @Test
    public void g_V_age_mean() {
        loadModern();
        Traversal<Vertex, Double> t1 = sqlgGraph.traversal().V().values("age").mean();
        Traversal<Vertex, Double> t2 = sqlgGraph.traversal().V().values("age").fold().mean(Scope.local);
        for (final Traversal<Vertex, Double> traversal : Arrays.asList(t1, t2)) {
            printTraversalForm(traversal);
            final Double mean = traversal.next();
            Assert.assertEquals(30.75, mean, 0.05);
            Assert.assertFalse(traversal.hasNext());
        }
    }

    @Test
    public void g_V_foo_mean() {
        loadModern();

        Traversal<Vertex, Number> t1 = sqlgGraph.traversal().V().values("foo").mean();
        Traversal<Vertex, Number> t2 =  sqlgGraph.traversal().V().values("foo").fold().mean(Scope.local);
        for (final Traversal<Vertex, Number> traversal : Arrays.asList(t1, t2)) {
            printTraversalForm(traversal);
            Assert.assertFalse(traversal.hasNext());
        }
    }

    @Test
    public void g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_meanX() {
        loadModern();
        final Traversal<Vertex, Map<String, Number>> traversal =  this.sqlgGraph.traversal().V().hasLabel("software")
                .<String, Number>group().by("name").by(bothE().values("weight").mean());
        printTraversalForm(traversal);
        Assert.assertTrue(traversal.hasNext());
        final Map<String, Number> map = traversal.next();
        assertFalse(traversal.hasNext());
        Assert.assertEquals(2, map.size());
        Assert.assertEquals(1.0, map.get("ripple"));
        Assert.assertEquals(1.0 / 3, map.get("lop"));
    }

    @Test
    public void testGroupOverOnePropertyMax() {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "A", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "B", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "A", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "B", "age", 4);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Map<String, Integer>> traversal = (DefaultTraversal) sqlgGraph.traversal()
                .V().hasLabel("Person")
                .<String, Integer>group().by("name").by(__.values("age").max());
        printTraversalForm(traversal);
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
    public void testGroupOverOnePropertyMin() {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "A", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "B", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "A", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "B", "age", 4);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Map<String, Integer>> traversal = (DefaultTraversal) sqlgGraph.traversal()
                .V().hasLabel("Person")
                .<String, Integer>group().by("name").by(__.values("age").min());
        printTraversalForm(traversal);
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
        this.sqlgGraph.addVertex(T.label, "Person", "name", "A", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "B", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "A", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "B", "age", 4);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Map<String, Long>> traversal = (DefaultTraversal) sqlgGraph.traversal()
                .V().hasLabel("Person")
                .<String, Long>group().by("name").by(__.values("age").sum());
        printTraversalForm(traversal);
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
        this.sqlgGraph.addVertex(T.label, "Person", "name", "A", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "B", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "A", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "B", "age", 4);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Map<String, Double>> traversal = (DefaultTraversal)sqlgGraph.traversal()
                .V().hasLabel("Person")
                .<String, Double>group().by("name").by(__.values("age").mean());
        printTraversalForm(traversal);
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
        this.sqlgGraph.addVertex(T.label, "Person", "name", "A", "surname", "C", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "B", "surname", "D", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "A", "surname", "C", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "B", "surname", "E", "age", 4);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "C", "surname", "E", "age", 5);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Map<List<String>, Integer>> traversal = (DefaultTraversal)this.sqlgGraph.traversal().V().hasLabel("Person")
                .<List<String>, Integer>group()
                .by(__.values("name", "surname").fold())
                .by(__.values("age").max());

        printTraversalForm(traversal);
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
        this.sqlgGraph.addVertex(T.label, "Person", "name", "A", "surname", "C", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "B", "surname", "D", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "A", "surname", "C", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "B", "surname", "E", "age", 4);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "C", "surname", "E", "age", 5);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Map<Map<String, List<String>>, Integer>> traversal = (DefaultTraversal)this.sqlgGraph.traversal().V().hasLabel("Person")
                .<Map<String, List<String>>, Integer>group()
                .by(__.valueMap("name", "surname"))
                .by(__.values("age").max());

        printTraversalForm(traversal);
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
        Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "name", "A");
        Vertex address1 = this.sqlgGraph.addVertex(T.label, "Address", "name", "A", "year", 2);
        Vertex address2 = this.sqlgGraph.addVertex(T.label, "Address", "name", "A", "year", 4);
        Vertex address3 = this.sqlgGraph.addVertex(T.label, "Address", "name", "C", "year", 6);
        Vertex address4 = this.sqlgGraph.addVertex(T.label, "Address", "name", "D", "year", 8);
        Vertex address5 = this.sqlgGraph.addVertex(T.label, "Address", "name", "D", "year", 7);
        Vertex address6 = this.sqlgGraph.addVertex(T.label, "Address", "name", "D", "year", 6);
        person.addEdge("livesAt", address1);
        person.addEdge("livesAt", address2);
        person.addEdge("livesAt", address3);
        person.addEdge("livesAt", address4);
        person.addEdge("livesAt", address5);
        person.addEdge("livesAt", address6);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Map<String, Integer>> traversal = (DefaultTraversal)this.sqlgGraph.traversal()
                .V().hasLabel("Person")
                .out("livesAt")
                .<String, Integer>group()
                .by("name")
                .by(__.values("year").max());

        printTraversalForm(traversal);
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
    public void testGroupByLabel() {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "A", "age", 10);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "B", "age", 20);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "C", "age", 100);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "D", "age", 40);

        this.sqlgGraph.addVertex(T.label, "Dog", "name", "A", "age", 10);
        this.sqlgGraph.addVertex(T.label, "Dog", "name", "B", "age", 200);
        this.sqlgGraph.addVertex(T.label, "Dog", "name", "C", "age", 30);
        this.sqlgGraph.addVertex(T.label, "Dog", "name", "D", "age", 40);

        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Map<String, Integer>> traversal = (DefaultTraversal)this.sqlgGraph.traversal().V().<String, Integer>group().by(T.label).by(__.values("age").max());
        printTraversalForm(traversal);
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
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "age", 5);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "age", 7);
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a4", "age", 5);
        a1.addEdge("aa", a2);
        a1.addEdge("aa", a3);
        a1.addEdge("aa", a4);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Integer> traversal = (DefaultTraversal) this.sqlgGraph.traversal().V(a1).out("aa").values("age").max();
        printTraversalForm(traversal);
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
    public void testDuplicatePathQuery2() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "age", 5);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "age", 7);
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a4", "age", 5);
        a1.addEdge("aa", a2);
        a1.addEdge("aa", a3);
        a1.addEdge("aa", a4);
        a2.addEdge("aa", a1);
        a2.addEdge("aa", a3);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Integer> traversal = (DefaultTraversal)this.sqlgGraph.traversal().V(a1).out("aa").out("aa").values("age").max();
        printTraversalForm(traversal);
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
    public void testGroupByDuplicatePaths() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "age", 2);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "age", 3);
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "age", 4);
        Vertex a5 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "age", 5);
        Vertex a6 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "age", 6);
        a1.addEdge("aa", a2);
        a1.addEdge("aa", a3);
        a1.addEdge("aa", a4);
        a1.addEdge("aa", a5);
        a1.addEdge("aa", a6);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Map<String, Integer>> traversal = (DefaultTraversal)this.sqlgGraph.traversal().V(a1).out().<String, Integer>group().by("name").by(__.values("age").max());
        printTraversalForm(traversal);
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
        this.sqlgGraph.addVertex(T.label, "A", "age", 1);
        this.sqlgGraph.addVertex(T.label, "B", "age", 2);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Integer> traversal = (DefaultTraversal)this.sqlgGraph.traversal().V().values("age").max();
        printTraversalForm(traversal);
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgMaxGlobalStep);

        Assert.assertEquals(2, traversal.next(), 0);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void duplicateQueryMin() {
        this.sqlgGraph.addVertex(T.label, "A", "age", 1);
        this.sqlgGraph.addVertex(T.label, "B", "age", 2);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Integer> traversal = (DefaultTraversal)this.sqlgGraph.traversal().V().values("age").min();
        printTraversalForm(traversal);
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgMinGlobalStep);

        Assert.assertEquals(1, traversal.next(), 0);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void duplicateQuerySum() {
        this.sqlgGraph.addVertex(T.label, "A", "age", 1);
        this.sqlgGraph.addVertex(T.label, "B", "age", 2);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Long> traversal = (DefaultTraversal)this.sqlgGraph.traversal().V().values("age").sum();
        printTraversalForm(traversal);
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgSumGlobalStep);

        Assert.assertEquals(3, traversal.next(), 0L);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void duplicateQueryMean() {
        this.sqlgGraph.addVertex(T.label, "A", "age", 1);

        this.sqlgGraph.addVertex(T.label, "B", "age", 2);
        this.sqlgGraph.addVertex(T.label, "B", "age", 2);
        this.sqlgGraph.addVertex(T.label, "B", "age", 2);
        this.sqlgGraph.addVertex(T.label, "B", "age", 2);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Double> traversal = (DefaultTraversal)this.sqlgGraph.traversal().V().values("age").mean();
        printTraversalForm(traversal);
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgAvgGlobalStep);

        Assert.assertEquals(1.8, traversal.next(), 0);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testDuplicateQueryNoLabel() {
        Vertex p1 = this.sqlgGraph.addVertex(T.label, "person", "age", 1);
        Vertex p2 = this.sqlgGraph.addVertex(T.label, "person", "age", 2);
        p1.addEdge("knows", p2);
        this.sqlgGraph.tx().commit();

        DefaultTraversal<Vertex, Double> traversal = (DefaultTraversal)this.sqlgGraph.traversal().V().hasLabel("person").out().out().values("age").max();
        printTraversalForm(traversal);
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgMaxGlobalStep);
        Assert.assertTrue(traversal.hasNext());
        Number m = traversal.next();
        //noinspection ConstantConditions
        Assert.assertTrue(m instanceof Double);
        Assert.assertEquals(Double.NaN, (Double) m, 0D);
        Assert.assertFalse(traversal.hasNext());
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void g_V_out_out_max() {
        loadModern();
        DefaultTraversal<Vertex, Integer> traversal = (DefaultTraversal)this.sqlgGraph.traversal().V().hasLabel("person").out().out().values("age").max();
        printTraversalForm(traversal);
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgMaxGlobalStep);
        Assert.assertTrue(traversal.hasNext());
        Number m = traversal.next();
        Assert.assertTrue(m instanceof Double);
        Assert.assertEquals(Double.NaN, (Double) m, 0D);
        Assert.assertFalse(traversal.hasNext());
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void g_V_repeatXoutX_timesX2X_age_max() {
        loadModern();
        DefaultTraversal<Vertex, Integer> traversal = (DefaultTraversal)this.sqlgGraph.traversal().V().hasLabel("person").repeat(__.out()).times(2).values("age").max();
        printTraversalForm(traversal);
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SqlgMaxGlobalStep);
        Assert.assertTrue(traversal.hasNext());
        Number m = traversal.next();
        Assert.assertTrue(m instanceof Double);
        Assert.assertEquals(Double.NaN, (Double) m, 0D);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void g_V_repeatXbothX_timesX5X_age_max() {
        loadModern();
        Traversal<Vertex, Integer> traversal = this.sqlgGraph.traversal().V().repeat(__.both()).times(5).values("age").max();
        printTraversalForm(traversal);
        checkResults(Collections.singletonList(35), traversal);
    }

    @Test
    public void g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_maxX() {
        loadModern();
        final Traversal<Vertex, Map<String, Number>> traversal = this.sqlgGraph.traversal()
                .V().hasLabel("software")
                .<String, Number>group().by("name").by(
                        __.bothE().values("weight").max()
                );
        printTraversalForm(traversal);
        Assert.assertTrue(traversal.hasNext());
        final Map<String, Number> map = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertEquals(2, map.size());
        Assert.assertEquals(1.0, map.get("ripple"));
        Assert.assertEquals(0.4, map.get("lop"));
    }

    @Test
    public void testMaxAgain() {
        this.sqlgGraph.addVertex(T.label, "Person", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 5);
        this.sqlgGraph.addVertex(T.label, "Dog", "age", 7);
        this.sqlgGraph.tx().commit();
        Traversal<Vertex, Integer> traversal = this.sqlgGraph.traversal().V().values("age").max();
        printTraversalForm(traversal);
        Integer max = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertEquals(7, max, 0);
    }

    @Test
    public void testMax2() {
        loadModern();
        Traversal<Vertex, Integer> traversal = this.sqlgGraph.traversal().V().values("age").max();
        printTraversalForm(traversal);
        checkResults(Collections.singletonList(35), traversal);
    }

    @Test
    public void g_V_outXfollowedByX_group_byXsongTypeX_byXbothE_group_byXlabelX_byXweight_sumXX() {
        loadGratefulDead();
        final Traversal<Vertex, Map<String, Map<String, Number>>> traversal = this.sqlgGraph.traversal()
                .V().out("followedBy")
                .<String, Map<String, Number>>group()
                .by("songType")
                .by(
                        __.bothE()
                                .group()
                                .by(T.label)
                                .by(__.values("weight").sum())
                );
        printTraversalForm(traversal);
        final Map<String, Map<String, Number>> map = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertEquals(3, map.size());
        Assert.assertTrue(map.containsKey(""));
        Assert.assertTrue(map.containsKey("original"));
        Assert.assertTrue(map.containsKey("cover"));
        //
        Map<String, Number> subMap = map.get("");
        Assert.assertEquals(1, subMap.size());
        Assert.assertEquals(179350, subMap.get("followedBy").intValue());
        //
        subMap = map.get("original");
//        Assert.assertEquals(3, subMap.size());
        Assert.assertEquals(2185613, subMap.get("followedBy").intValue());
//        Assert.assertEquals(0, subMap.get("writtenBy").intValue());
//        Assert.assertEquals(0, subMap.get("sungBy").intValue());
        //
        subMap = map.get("cover");
//        Assert.assertEquals(3, subMap.size());
        Assert.assertEquals(777982, subMap.get("followedBy").intValue());
//        Assert.assertEquals(0, subMap.get("writtenBy").intValue());
//        Assert.assertEquals(0, subMap.get("sungBy").intValue());
    }

    @Test
    public void testSummingNothing() {
        this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1);
        this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "age", 1);
        this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "age", 1);
        this.sqlgGraph.addVertex(T.label, "A", "name", "a4", "age", 1);
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Map<String, Integer>> traversal = this.sqlgGraph.traversal()
                .V().has("name", "a5")
                .<String, Integer>group().by(T.label).by(__.values("age").sum());
        printTraversalForm(traversal);
        Assert.assertTrue(traversal.hasNext());
        System.out.println(traversal.next());
    }

    @Test
    public void testCount() {
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        Traversal<Vertex, Long> traversal = this.sqlgGraph.traversal().V().count();
        printTraversalForm(traversal);
        Assert.assertEquals(4, traversal.next(), 0);
    }

    @Test
    public void testCountMultipleLabels() {
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        this.sqlgGraph.addVertex(T.label, "B", "name", "b4");
        this.sqlgGraph.tx().commit();
        Traversal<Vertex, Long> traversal = this.sqlgGraph.traversal().V().count();
        printTraversalForm(traversal);
        Assert.assertEquals(8, traversal.next(), 0);
    }

    @Test
    public void testCountDuplicatePaths() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a4");
        a1.addEdge("aa", a2);
        a2.addEdge("aa", a3);
        a2.addEdge("aa", a4);
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Long> traversal = this.sqlgGraph.traversal().V(a1).out().out().count();
        printTraversalForm(traversal);
        Assert.assertEquals(2, traversal.next(), 0);
    }

    @Test
    public void testMaxDuplicatePaths() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "age", 2);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "age", 3);
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a4", "age", 4);
        a1.addEdge("aa", a2);
        a2.addEdge("aa", a3);
        a2.addEdge("aa", a4);
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Integer> traversal = this.sqlgGraph.traversal().V(a1).out().out().values("age").max();
        printTraversalForm(traversal);
        Assert.assertEquals(4, traversal.next(), 0);
    }

}
