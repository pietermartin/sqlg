package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.predicate.Text;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.util.List;

/**
 * Date: 2017/02/10
 * Time: 8:19 PM
 */
public class TestGremlinCompileTextPredicate extends BaseTest {

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
        BaseTest.beforeClass();
        if (configuration.getString("jdbc.url").contains("postgresql")) {
            configuration.addProperty("distributed", true);
        }
    }

    @Test
    public void testTextContains() throws InterruptedException {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "aaaaa");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "abcd");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        this.sqlgGraph.tx().commit();
        testTextContains_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testTextContains_assert(this.sqlgGraph1);
        }
    }

    private void testTextContains_assert(SqlgGraph sqlgGraph) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal)sqlgGraph.traversal().V().hasLabel("Person").has("name", Text.contains("a"));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.contains("aaa"));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(1, traversal1.getSteps().size());

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.contains("abc"));
        Assert.assertEquals(2, traversal2.getSteps().size());
        vertices = traversal2.toList();
        Assert.assertEquals(1, traversal2.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.contains("acd"));
        Assert.assertEquals(2, traversal3.getSteps().size());
        vertices = traversal3.toList();
        Assert.assertEquals(1, traversal3.getSteps().size());
        Assert.assertEquals(0, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal4 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.contains("ohn"));
        Assert.assertEquals(2, traversal4.getSteps().size());
        vertices = traversal4.toList();
        Assert.assertEquals(1, traversal4.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal5 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.contains("Ohn"));
        Assert.assertEquals(2, traversal5.getSteps().size());
        vertices = traversal5.toList();
        Assert.assertEquals(1, traversal5.getSteps().size());
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void showTextPredicate() throws InterruptedException {
        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "John XXX Doe");
        Vertex peter = this.sqlgGraph.addVertex(T.label, "Person", "name", "Peter YYY Snow");
        this.sqlgGraph.tx().commit();
        testTextPredicate_assert(this.sqlgGraph, john);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testTextPredicate_assert(this.sqlgGraph1, john);
        }
    }

    private void testTextPredicate_assert(SqlgGraph sqlgGraph, Vertex john) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.contains("XXX"));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> persons = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals(john, persons.get(0));
    }

    @Test
    public void testTextNotContains() throws InterruptedException {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "aaaaa");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "abcd");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        this.sqlgGraph.tx().commit();
        testTextNotContains_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testTextNotContains_assert(this.sqlgGraph1);
        }
    }

    private void testTextNotContains_assert(SqlgGraph sqlgGraph) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.ncontains("a"));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.ncontains("aaa"));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.ncontains("abc"));
        Assert.assertEquals(2, traversal2.getSteps().size());
        vertices = traversal2.toList();
        Assert.assertEquals(1, traversal2.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.ncontains("acd"));
        Assert.assertEquals(2, traversal3.getSteps().size());
        vertices = traversal3.toList();
        Assert.assertEquals(1, traversal3.getSteps().size());
        Assert.assertEquals(3, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal4 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.ncontains("ohn"));
        Assert.assertEquals(2, traversal4.getSteps().size());
        vertices = traversal4.toList();
        Assert.assertEquals(1, traversal4.getSteps().size());
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testTextContainsCIS() throws InterruptedException {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "aaaaa");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "abcd");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        this.sqlgGraph.tx().commit();
        testTextContainsCIS_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testTextContainsCIS_assert(this.sqlgGraph1);
        }
    }

    private void testTextContainsCIS_assert(SqlgGraph sqlgGraph) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.containsCIS("A"));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.containsCIS("AAA"));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.containsCIS("ABC"));
        Assert.assertEquals(2, traversal2.getSteps().size());
        vertices = traversal2.toList();
        Assert.assertEquals(1, traversal2.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.containsCIS("ACD"));
        Assert.assertEquals(2, traversal3.getSteps().size());
        vertices = traversal3.toList();
        Assert.assertEquals(1, traversal3.getSteps().size());
        Assert.assertEquals(0, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal4 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.containsCIS("OHN"));
        Assert.assertEquals(2, traversal4.getSteps().size());
        vertices = traversal4.toList();
        Assert.assertEquals(1, traversal4.getSteps().size());
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testTextNContainsCIS() throws InterruptedException {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "aaaaa");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "abcd");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        this.sqlgGraph.tx().commit();
        testTextNContainsCIS_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testTextNContainsCIS_assert(this.sqlgGraph1);
        }
    }

    private void testTextNContainsCIS_assert(SqlgGraph sqlgGraph) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.ncontainsCIS("A"));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.ncontainsCIS("AAA"));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.ncontainsCIS("ABC"));
        Assert.assertEquals(2, traversal2.getSteps().size());
        vertices = traversal2.toList();
        Assert.assertEquals(1, traversal2.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.ncontainsCIS("ACD"));
        Assert.assertEquals(2, traversal3.getSteps().size());
        vertices = traversal3.toList();
        Assert.assertEquals(1, traversal3.getSteps().size());
        Assert.assertEquals(3, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal4 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.ncontainsCIS("OHN"));
        Assert.assertEquals(2, traversal4.getSteps().size());
        vertices = traversal4.toList();
        Assert.assertEquals(1, traversal4.getSteps().size());
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testTextStartsWith() throws InterruptedException {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "aaaaa");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "abcd");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        this.sqlgGraph.tx().commit();
        testTextStartWith_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testTextStartWith_assert(this.sqlgGraph1);
        }
    }

    private void testTextStartWith_assert(SqlgGraph sqlgGraph) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.startsWith("a"));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.startsWith("aaa"));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.startsWith("abc"));
        Assert.assertEquals(2, traversal2.getSteps().size());
        vertices = traversal2.toList();
        Assert.assertEquals(1, traversal2.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.startsWith("acd"));
        Assert.assertEquals(2, traversal3.getSteps().size());
        vertices = traversal3.toList();
        Assert.assertEquals(1, traversal3.getSteps().size());
        Assert.assertEquals(0, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal4 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.startsWith("ohn"));
        Assert.assertEquals(2, traversal4.getSteps().size());
        vertices = traversal4.toList();
        Assert.assertEquals(1, traversal4.getSteps().size());
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testTextNStartsWith() throws InterruptedException {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "aaaaa");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "abcd");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        this.sqlgGraph.tx().commit();
        testTextNStartsWith_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testTextNStartsWith_assert(this.sqlgGraph1);
        }
    }

    private void testTextNStartsWith_assert(SqlgGraph sqlgGraph) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.nstartsWith("a"));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.nstartsWith("aaa"));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.nstartsWith("abc"));
        Assert.assertEquals(2, traversal2.getSteps().size());
        vertices = traversal2.toList();
        Assert.assertEquals(1, traversal2.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.nstartsWith("acd"));
        Assert.assertEquals(2, traversal3.getSteps().size());
        vertices = traversal3.toList();
        Assert.assertEquals(1, traversal3.getSteps().size());
        Assert.assertEquals(3, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal4 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.nstartsWith("ohn"));
        Assert.assertEquals(2, traversal4.getSteps().size());
        vertices = traversal4.toList();
        Assert.assertEquals(1, traversal4.getSteps().size());
        Assert.assertEquals(3, vertices.size());
    }

    @Test
    public void testTextEndsWith() throws InterruptedException {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "aaaaa");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "abcd");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        this.sqlgGraph.tx().commit();
        testTextEndsWith_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testTextEndsWith_assert(this.sqlgGraph1);
        }
    }

    private void testTextEndsWith_assert(SqlgGraph sqlgGraph) {
        DefaultGraphTraversal<Vertex, Vertex> has = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.endsWith("a"));
        Assert.assertEquals(2, has.getSteps().size());
        printTraversalForm(has);
        Assert.assertEquals(1, has.getSteps().size());
        List<Vertex> vertices = has.toList();
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.endsWith("aaa"));
        Assert.assertEquals(2, traversal.getSteps().size());
        vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.endsWith("abc"));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(0, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V().hasLabel("Person").has("name", Text.endsWith("acd"));
        Assert.assertEquals(2, traversal2.getSteps().size());
        vertices = traversal2.toList();
        Assert.assertEquals(1, traversal2.getSteps().size());
        Assert.assertEquals(0, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>)sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.endsWith("ohn"));
        Assert.assertEquals(2, traversal3.getSteps().size());
        vertices = traversal3.toList();
        Assert.assertEquals(1, traversal3.getSteps().size());
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testTextNEndsWith() throws InterruptedException {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "aaaaa");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "abcd");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        this.sqlgGraph.tx().commit();
        testTextNEndsWith_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testTextNEndsWith_assert(this.sqlgGraph1);
        }
    }

    private void testTextNEndsWith_assert(SqlgGraph sqlgGraph) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V().hasLabel("Person").has("name", Text.nendsWith("a"));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.nendsWith("aaa"));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.nendsWith("abc"));
        Assert.assertEquals(2, traversal2.getSteps().size());
        vertices = traversal2.toList();
        Assert.assertEquals(1, traversal2.getSteps().size());
        Assert.assertEquals(3, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.nendsWith("acd"));
        Assert.assertEquals(2, traversal3.getSteps().size());
        vertices = traversal3.toList();
        Assert.assertEquals(1, traversal3.getSteps().size());
        Assert.assertEquals(3, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal4 = (DefaultGraphTraversal<Vertex, Vertex>)sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.nendsWith("ohn"));
        Assert.assertEquals(2, traversal4.getSteps().size());
        vertices = traversal4.toList();
        Assert.assertEquals(1, traversal4.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal5 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", Text.nendsWith("D"));
        Assert.assertEquals(2, traversal5.getSteps().size());
        vertices = traversal5.toList();
        Assert.assertEquals(1, traversal5.getSteps().size());
        Assert.assertEquals(3, vertices.size());
    }
}
