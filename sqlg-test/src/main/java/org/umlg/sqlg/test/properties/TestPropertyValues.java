package org.umlg.sqlg.test.properties;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.step.SqlgGraphStep;
import org.umlg.sqlg.step.SqlgPropertiesStep;
import org.umlg.sqlg.step.SqlgVertexStep;
import org.umlg.sqlg.structure.SqlgElement;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;

/**
 * test behavior on property values
 *
 * @author JP Moresmau
 * @author Pieter Martin (https://github.com/pietermartin)
 */
public class TestPropertyValues extends BaseTest {

    @Test
    public void testSelectFollowedByValues() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "xxxx", "c1", "yyyy", "y1", "zzzz", "z1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "xxxx", "c2", "yyyy", "y2", "zzzz", "z2");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "name", "d2");
        a1.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        b2.addEdge("bc", c2);
        c1.addEdge("cd", d1);
        c2.addEdge("cd", d2);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, String> traversal = (DefaultGraphTraversal<Vertex, String>)this.sqlgGraph.traversal().V()
                .hasLabel("C")
                .has("yyyy", "y1")
                .as("c")
                .in("bc")
                .in("ab")
                .has("name", "a1")
                .as("a")
                .<Vertex>select("a", "c")
                .select("c")
                .<String>values("xxxx");
        printTraversalForm(traversal);
        Assert.assertEquals(6, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof IdentityStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof IdentityStep);
        Assert.assertTrue(traversal.getSteps().get(3) instanceof SelectStep);
        Assert.assertTrue(traversal.getSteps().get(4) instanceof SelectOneStep);
        Assert.assertTrue(traversal.getSteps().get(5) instanceof SqlgPropertiesStep);
        List<String> names = traversal.toList();
        Assert.assertEquals(1, names.size());
        Assert.assertEquals("c1", names.get(0));
        checkRestrictedProperties(SqlgGraphStep.class, traversal, 0,  "xxxx");
    }

    @Test
    public void testSelectOneFollowedByValues() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "xxxx", "c1", "yyyy", "y1", "zzzz", "z1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "xxxx", "c2", "yyyy", "y2", "zzzz", "z2");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "name", "d2");
        a1.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        b2.addEdge("bc", c2);
        c1.addEdge("cd", d1);
        c2.addEdge("cd", d2);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, String> traversal = (DefaultGraphTraversal<Vertex, String>)this.sqlgGraph.traversal().V()
                .hasLabel("C")
                .has("yyyy", "y1")
                .as("c")
                .in("bc")
                .in("ab")
                .has("name", "a1")
                .<Vertex>select("c")
                .<String>values("xxxx");
        printTraversalForm(traversal);
        Assert.assertEquals(4, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof IdentityStep);
        Assert.assertTrue(traversal.getSteps().get(2) instanceof SelectOneStep);
        Assert.assertTrue(traversal.getSteps().get(3) instanceof SqlgPropertiesStep);
        List<String> names = traversal.toList();
        Assert.assertEquals(1, names.size());
        Assert.assertEquals("c1", names.get(0));
        checkRestrictedProperties(SqlgGraphStep.class, traversal, 0,  "xxxx");
    }

    @Test
    public void testOptimizePastSelect() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1", "surname", "s1");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "name", "d2", "surname", "s2");
        a1.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        b2.addEdge("bc", c2);
        c1.addEdge("cd", d1);
        c2.addEdge("cd", d2);
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, String> traversal = this.sqlgGraph.traversal()
                .V().hasLabel("C")
                .has("name", "c2")
                .as("c")
                .in("bc")
                .in("ab")
                .has("name", "a2")
                .select("c")
                .out("cd")
                .has("name", "d2")
                .values("surname");

        printTraversalForm(traversal);
        List<String> surnames = traversal.toList();
        Assert.assertEquals(1, surnames.size());
        Assert.assertEquals("s2", surnames.get(0));
        checkRestrictedProperties(SqlgVertexStep.class, traversal, 0, "surname");
    }

    @Test
    public void testMultipleSelect() {
        Vertex vA = sqlgGraph.addVertex(T.label, "A", "name", "root");
        Vertex vI = sqlgGraph.addVertex(T.label, "I", "name", "item1");
        vA.addEdge("likes", vI, "howMuch", 5, "who", "Joe");
        this.sqlgGraph.tx().commit();
        Object id0 = vI.id();
        GraphTraversal<Vertex, Map<String, Object>> gt = sqlgGraph.traversal().V()
                .hasLabel("A")
                .has("name", "root")
                .outE("likes")
                .as("e")
                .values("howMuch").as("stars")
                .select("e")
                .values("who").as("user")
                .select("e")
                .inV()
                .id().as("item")
                .select("user", "stars", "item");
        printTraversalForm(gt);
        Assert.assertTrue(gt.hasNext());
        Map<String, Object> m = gt.next();
        Assert.assertEquals(new Integer(5), m.get("stars"));
        Assert.assertEquals("Joe", m.get("user"));
        Assert.assertEquals(id0, m.get("item"));
    }

    /**
     * If the order() does not happen on the database, i.e. in java code then the property also needs to be present.
     */
    @Test
    public void testInMemoryOrderByValues() {
        loadModern();
        final Traversal<Vertex, String> traversal = this.sqlgGraph.traversal().V().both().hasLabel("person").order().by("age", Order.desc).limit(5).values("name");
        printTraversalForm(traversal);
        checkOrderedResults(Arrays.asList("peter", "josh", "josh", "josh", "marko"), traversal);
    }

    @Test
    public void testValueMapOneObject() {
        loadModern();
        final Traversal<Vertex, Map<Object, Object>> traversal = sqlgGraph.traversal().V().hasLabel("person").valueMap("name");
        checkColumnsNotPresent(traversal, "age");
        checkRestrictedProperties(SqlgGraphStep.class, traversal, 0, "name");
        Set<String> names = new HashSet<>();
        while (traversal.hasNext()) {
            Map<Object, Object> m = traversal.next();
            Assert.assertNotNull(m);
            Assert.assertEquals(1, m.size());
            Assert.assertTrue(m.containsKey("name"));
            Object v = m.get("name");
            // "It is important to note that the map of a vertex maintains a list of values for each key."
            Assert.assertTrue(v instanceof List<?>);
            List<?> l = (List<?>) v;
            Assert.assertEquals(1, l.size());
            Object v1 = l.get(0);
            Assert.assertTrue(v1 instanceof String);
            names.add((String) v1);
        }
        Assert.assertEquals(new HashSet<>(Arrays.asList("marko", "vadas", "josh", "peter")), names);
    }

    @Test
    public void testValueMapAllObject() {
        loadModern();
        final Traversal<Vertex, Map<Object, Object>> traversal = sqlgGraph.traversal().V().hasLabel("person").valueMap();
        printTraversalForm(traversal);
        checkNoRestrictedProperties(traversal);
        Set<String> names = new HashSet<>();
        Set<Integer> ages = new HashSet<>();
        while (traversal.hasNext()) {
            Map<Object, Object> m = traversal.next();
            Assert.assertNotNull(m);
            Assert.assertEquals(2, m.size());
            Assert.assertTrue(m.containsKey("name"));
            Object v = m.get("name");
            // "It is important to note that the map of a vertex maintains a list of values for each key."
            Assert.assertTrue(v instanceof List<?>);
            List<?> l = (List<?>) v;
            Assert.assertEquals(1, l.size());
            Object v1 = l.get(0);
            Assert.assertTrue(v1 instanceof String);
            names.add((String) v1);
            Assert.assertTrue(m.containsKey("age"));
            v = m.get("age");
            // "It is important to note that the map of a vertex maintains a list of values for each key."
            Assert.assertTrue(v instanceof List<?>);
            l = (List<?>) v;
            Assert.assertEquals(1, l.size());
            v1 = l.get(0);
            Assert.assertTrue(v1 instanceof Integer);
            ages.add((Integer) v1);
        }
        Assert.assertEquals(new HashSet<>(Arrays.asList("marko", "vadas", "josh", "peter")), names);
        Assert.assertEquals(new HashSet<>(Arrays.asList(29, 27, 32, 35)), ages);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testValueMapAliasVertex() {
        loadModern();
        final Traversal<Vertex, Map<String, Object>> traversal = sqlgGraph.traversal()
                .V().hasLabel("person").as("a")
                .valueMap("name").as("b")
                .select("a", "b");
        printTraversalForm(traversal);
        checkNoRestrictedProperties(traversal);
        Set<String> names1 = new HashSet<>();
        Set<String> names2 = new HashSet<>();
        Set<Integer> ages = new HashSet<>();
        while (traversal.hasNext()) {
            Map<String, Object> m = traversal.next();
            Assert.assertNotNull(m);
            Assert.assertEquals(2, m.size());
            Assert.assertTrue(m.containsKey("a"));
            Assert.assertTrue(m.containsKey("b"));
            Vertex v = (Vertex) m.get("a");
            Assert.assertTrue(v.property("name").isPresent());
            Assert.assertTrue(v.property("age").isPresent());
            names1.add((String) v.property("name").value());
            ages.add((Integer) v.property("age").value());

            Map<String, Object> m2 = (Map<String, Object>) m.get("b");
            Object o = m2.get("name");
            // "It is important to note that the map of a vertex maintains a list of values for each key."
            Assert.assertTrue(o instanceof List<?>);
            List<?> l = (List<?>) o;
            Assert.assertEquals(1, l.size());
            Object v1 = l.get(0);
            Assert.assertTrue(v1 instanceof String);
            names2.add((String) v1);
        }
        Assert.assertEquals(names1, names2);
        Assert.assertEquals(new HashSet<>(Arrays.asList("marko", "vadas", "josh", "peter")), names1);
        Assert.assertEquals(new HashSet<>(Arrays.asList(29, 27, 32, 35)), ages);
    }

    @Test
    public void testValueMapAlias() {
        loadModern();
        final Traversal<Vertex, Map<String, Object>> traversal = sqlgGraph.traversal().V().hasLabel("person").valueMap("name").as("b").select("b");
        checkColumnsNotPresent(traversal, "age");
        checkRestrictedProperties(SqlgGraphStep.class, traversal, 0,"name");
        Set<String> names = new HashSet<>();
        while (traversal.hasNext()) {
            Map<String, Object> m = traversal.next();
            Assert.assertNotNull(m);
            Assert.assertEquals(1, m.size());
            Object o = m.get("name");
            // "It is important to note that the map of a vertex maintains a list of values for each key."
            Assert.assertTrue(o instanceof List<?>);
            List<?> l = (List<?>) o;
            Assert.assertEquals(1, l.size());
            Object v1 = l.get(0);
            Assert.assertTrue(v1 instanceof String);
            names.add((String) v1);
        }
        Assert.assertEquals(new HashSet<>(Arrays.asList("marko", "vadas", "josh", "peter")), names);
    }

    @Test
    public void testValuesOne() {
        loadModern();
        final Traversal<Vertex, String> traversal = sqlgGraph.traversal().V().hasLabel("person").values("name");
        checkColumnsNotPresent(traversal, "age");
        checkRestrictedProperties(SqlgGraphStep.class, traversal, 0, "name");
        Set<String> names = new HashSet<>();
        while (traversal.hasNext()) {
            names.add(traversal.next());
        }
        Assert.assertEquals(new HashSet<>(Arrays.asList("marko", "vadas", "josh", "peter")), names);
    }

    @Test
    public void testValuesAll() {
        loadModern();
        final Traversal<Vertex, Object> traversal = sqlgGraph.traversal().V().hasLabel("person").values();
        printTraversalForm(traversal);
        checkNoRestrictedProperties(traversal);
        Set<Object> values = new HashSet<>();
        while (traversal.hasNext()) {
            values.add(traversal.next());
        }
        Assert.assertEquals(new HashSet<>(Arrays.asList("marko", "vadas", "josh", "peter", 29, 27, 32, 35)), values);
    }


    @Test
    public void testValuesOneWhere() {
        loadModern();
        final Traversal<Vertex, String> traversal = sqlgGraph.traversal().V().hasLabel("person").has("age", 29).values("name");
        checkColumnsNotPresent(traversal, "age");
        checkRestrictedProperties(SqlgGraphStep.class, traversal, 0, "name");
        Set<String> names = new HashSet<>();
        while (traversal.hasNext()) {
            names.add(traversal.next());
        }
        Assert.assertEquals(new HashSet<>(Arrays.asList("marko")), names);
    }

    @Test
    public void g_V_hasLabelXpersonX_order_byXageX_skipX1X_valuesXnameX() {
        loadModern();

        final Traversal<Vertex, String> traversal = this.sqlgGraph.traversal()
                .V().hasLabel("person")
                .order().by("age")
                .skip(1)
                .values("name");
        printTraversalForm(traversal);
        // name because explicitly requested, age because we order on it
        checkRestrictedProperties(SqlgGraphStep.class, traversal, 0, "name", "age");
        Assert.assertTrue(traversal.hasNext());
        Assert.assertEquals(Arrays.asList("marko", "josh", "peter"), traversal.toList());
    }

    @Test
    public void testOut() {
        loadModern();

        final Traversal<Vertex, String> traversal = this.sqlgGraph.traversal()
                .V().hasLabel("person")
                .out("created")
                .values("name");
        checkColumnsNotPresent(traversal, "language");
        checkRestrictedProperties(SqlgGraphStep.class, traversal, 1, "name");
        Set<String> names = new HashSet<>();
        while (traversal.hasNext()) {
            names.add(traversal.next());
        }
        Assert.assertEquals(new HashSet<>(Arrays.asList("lop", "ripple")), names);
    }

    @Test
    public void g_V_both_name_order_byXa_bX_dedup_value() {
        loadModern();
        @SuppressWarnings("ComparatorCombinators") final Traversal<Vertex, String> traversal = this.sqlgGraph.traversal()
                .V()
                .both()
                .<String>properties("name")
                .order().by((a, b) -> a.value().compareTo(b.value()))
                .dedup().value();
        printTraversalForm(traversal);
        final List<String> names = traversal.toList();
        Assert.assertEquals(6, names.size());
        Assert.assertEquals("josh", names.get(0));
        Assert.assertEquals("lop", names.get(1));
        Assert.assertEquals("marko", names.get(2));
        Assert.assertEquals("peter", names.get(3));
        Assert.assertEquals("ripple", names.get(4));
        Assert.assertEquals("vadas", names.get(5));
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void g_V_valuesXnameX_order_tail() {
        loadModern();
        final Traversal<Vertex, String> traversal = this.sqlgGraph.traversal().V().<String>values("name").order().tail();
        printTraversalForm(traversal);
        Assert.assertEquals(Arrays.asList("vadas"), traversal.toList());
    }

    /**
     * check provided columns/properties are not selected in the SQL
     *
     * @param t          the traversal
     * @param properties the properties to check for absence
     */
    private void checkColumnsNotPresent(Traversal<?, ?> t, String... properties) {
        String sql = getSQL(t);
        Assert.assertNotNull(sql);
        sql = sql.trim();
        Assert.assertTrue(sql.startsWith("SELECT"));
        int ix = sql.indexOf("FROM");
        Assert.assertTrue(ix > 0);
        String select = sql.substring(0, ix);
        for (String p : properties) {
            Assert.assertFalse(select.contains(p));
        }
    }

    /**
     * check the replaced steps has the specified restricted properties
     *
     * @param t          the traversal
     * @param properties the properties
     */
    @SuppressWarnings({"resource", "unchecked"})
    private void checkRestrictedProperties(Class<? extends Step> stepType, Traversal<?, ?> t, int replacedStepCount, String... properties) {
        boolean found = false;
        for (Step<?, ?> s : ((Traversal.Admin<?, ?>) t).getSteps()) {
            if (stepType == SqlgGraphStep.class && s instanceof SqlgGraphStep) {
                SqlgGraphStep<?, SqlgElement> gs = (SqlgGraphStep<?, SqlgElement>) s;
                ReplacedStep<?, ?> rs = gs.getReplacedSteps().get(replacedStepCount);
                Assert.assertEquals(new HashSet<>(Arrays.asList(properties)), rs.getRestrictedProperties());
                found = true;
            } else if (stepType == SqlgVertexStep.class && s instanceof SqlgVertexStep) {
                SqlgVertexStep<SqlgElement> gs = (SqlgVertexStep<SqlgElement>) s;
                ReplacedStep<?, ?> rs = gs.getReplacedSteps().get(gs.getReplacedSteps().size() - 1);
                Assert.assertEquals(new HashSet<>(Arrays.asList(properties)), rs.getRestrictedProperties());
                found = true;
            }
            if (found) {
                break;
            }
        }
        Assert.assertTrue(found);
    }

    /**
     * check the replaced steps has the specified restricted properties
     *
     * @param t the traversal, EVALUATED (ie call printTraversalForm or getSQL first)
     */
    @SuppressWarnings({"resource", "unchecked"})
    private void checkNoRestrictedProperties(Traversal<?, ?> t) {
        boolean found = false;
        for (Step<?, ?> s : ((Traversal.Admin<?, ?>) t).getSteps()) {
            if (s instanceof SqlgGraphStep) {
                SqlgGraphStep<?, SqlgElement> gs = (SqlgGraphStep<?, SqlgElement>) s;
                ReplacedStep<?, ?> rs = gs.getReplacedSteps().get(gs.getReplacedSteps().size() - 1);
                Assert.assertNull(String.valueOf(rs.getRestrictedProperties()), rs.getRestrictedProperties());
                found = true;
            }
        }
        Assert.assertTrue(found);
    }

}
