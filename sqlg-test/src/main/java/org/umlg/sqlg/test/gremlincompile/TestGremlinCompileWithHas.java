package org.umlg.sqlg.test.gremlincompile;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Date: 2015/01/19
 * Time: 6:22 AM
 */
public class TestGremlinCompileWithHas extends BaseTest {

    @BeforeClass
    public static void beforeClass() {
        BaseTest.beforeClass();
        if (isPostgres()) {
            configuration.addProperty("distributed", true);
        }
    }

    @Test
    public void testMultipleHasWithinUserSuppliedIds() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "TestHierarchy",
                new LinkedHashMap<String, PropertyType>() {{
                    put("column1", PropertyType.varChar(100));
                    put("column2", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("column1", "column2"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "TestHierarchy", "column1", "a1", "column2", "a2", "name", "name1");
        this.sqlgGraph.addVertex(T.label, "TestHierarchy", "column1", "b1", "column2", "b2", "name", "name1");
        this.sqlgGraph.addVertex(T.label, "TestHierarchy", "column1", "c1", "column2", "c2", "name", "name1");
        this.sqlgGraph.addVertex(T.label, "TestHierarchy", "column1", "d1", "column2", "d2", "name", "name1");
        this.sqlgGraph.tx().commit();

        List<String> values1 = Collections.singletonList("");
        List<String> values2 = Collections.singletonList("");
        List<Vertex> vertexList = this.sqlgGraph.traversal().V()
                .hasLabel("TestHierarchy")
                .has("column1", P.within(values1))
                .has("column2", P.within(values2))
                .toList();
        Assert.assertEquals(0, vertexList.size());

        values1 = Arrays.asList("a1", "b1", "c1");
        values2 = Arrays.asList("a2", "b2", "c2");

        vertexList = this.sqlgGraph.traversal().V()
                .hasLabel("TestHierarchy")
                .has("column1", P.within(values1))
                .has("column2", P.within(values2))
                .toList();
        Assert.assertEquals(3, vertexList.size());

        values1 = Arrays.asList("a1");
        values2 = Arrays.asList("a2", "b2", "c2");
        vertexList = this.sqlgGraph.traversal().V()
                .hasLabel("TestHierarchy")
                .has("column1", P.within(values1))
                .has("column2", P.within(values2))
                .toList();
        Assert.assertEquals(1, vertexList.size());
    }

    @Test
    public void testHasPropertyWithLabel() {
        this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name").as("a").<Vertex>select("a").toList();
        Assert.assertEquals(2, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("A").hasNot("name").as("a").<Vertex>select("a").toList();
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testHasIdRecompilation() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();

        testHasIdRecompilation_assert(this.sqlgGraph, a1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testHasIdRecompilation_assert(this.sqlgGraph1, a1);
        }
    }

    private void testHasIdRecompilation_assert(SqlgGraph sqlgGraph, Vertex a1) {
        DefaultGraphTraversal<Vertex, Vertex> gt1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(a1.id());
        Assert.assertEquals(1, gt1.getSteps().size());
        DefaultGraphTraversal<Vertex, Vertex> gt2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V().hasId(a1.id());
        Assert.assertEquals(2, gt2.getSteps().size());

        List<Vertex> vertices1 = gt1.toList();
        Assert.assertEquals(1, gt1.getSteps().size());
        Assert.assertEquals(1, vertices1.size());
        Assert.assertEquals(a1, vertices1.get(0));

        List<Vertex> vertices2 = gt2.toList();
        Assert.assertEquals(1, gt2.getSteps().size());
        Assert.assertEquals(1, vertices2.size());
        Assert.assertEquals(a1, vertices2.get(0));
        Assert.assertEquals(gt1, gt2);
    }

    @Test
    public void testHasIdIn() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C");
        b1.addEdge("ab", a1);
        b2.addEdge("ab", a1);
        b3.addEdge("ab", a1);
        b4.addEdge("ab", a1);
        c1.addEdge("ac", a1);
        c2.addEdge("ac", a1);
        c3.addEdge("ac", a1);
        c4.addEdge("ac", a1);
        this.sqlgGraph.tx().commit();

        testHasIdIn_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testHasIdIn_assert(this.sqlgGraph1);
        }
    }

    private void testHasIdIn_assert(SqlgGraph sqlgGraph) {
        long start = sqlgGraph.getSqlDialect().getPrimaryKeyStartValue();
        RecordId recordIda1 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "A"), start);
        RecordId recordIda2 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "A"), start + 1L);
        RecordId recordIda3 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "A"), start + 2L);
        RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "A"), start + 3L);
        RecordId recordIdb1 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "B"), start);
        RecordId recordIdb2 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "B"), start + 1L);
        RecordId recordIdb3 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "B"), start + 2L);
        RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "B"), start + 3L);
        RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "C"), start);
        RecordId recordIdc2 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "C"), start + 1L);
        RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "C"), start + 2L);
        RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "C"), start + 3L);

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1).hasLabel("A");
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1).has(T.id, P.within(recordIda2, recordIdb1));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(0, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V().has(T.id, P.within(recordIda1, recordIda2, recordIdb1));
        Assert.assertEquals(2, traversal2.getSteps().size());
        vertices = traversal2.toList();
        Assert.assertEquals(1, traversal2.getSteps().size());
        Assert.assertEquals(3, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1, recordIda2, recordIda3, recordIdb1);
        Assert.assertEquals(1, traversal3.getSteps().size());
        vertices = traversal3.toList();
        Assert.assertEquals(1, traversal3.getSteps().size());
        Assert.assertEquals(4, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal4 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V().has(T.id, P.within(recordIda1));
        Assert.assertEquals(2, traversal4.getSteps().size());
        vertices = traversal4.toList();
        Assert.assertEquals(1, traversal4.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal5 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1).in().hasId(recordIdb1);
        Assert.assertEquals(3, traversal5.getSteps().size());
        vertices = traversal5.toList();
        Assert.assertEquals(1, traversal5.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal6 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1, recordIda2, recordIda3).in().hasId(recordIdb1, recordIdb2, recordIdb3);
        Assert.assertEquals(3, traversal6.getSteps().size());
        vertices = traversal6.toList();
        Assert.assertEquals(1, traversal6.getSteps().size());
        Assert.assertEquals(3, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal7 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1).in().hasId(recordIda1);
        Assert.assertEquals(3, traversal7.getSteps().size());
        vertices = traversal7.toList();
        Assert.assertEquals(1, traversal7.getSteps().size());
        Assert.assertEquals(0, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal8 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1).in().hasId(recordIdb1);
        Assert.assertEquals(3, traversal8.getSteps().size());
        vertices = traversal8.toList();
        Assert.assertEquals(1, traversal8.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal9 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1).in().hasId(recordIdb1, recordIdb2);
        Assert.assertEquals(3, traversal9.getSteps().size());
        vertices = traversal9.toList();
        Assert.assertEquals(1, traversal9.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal10 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1).in().hasId(recordIdb1.toString());
        Assert.assertEquals(3, traversal10.getSteps().size());
        vertices = traversal10.toList();
        Assert.assertEquals(1, traversal10.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal11 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1).in().hasId(recordIdb1.toString(), recordIdb2.toString());
        Assert.assertEquals(3, traversal11.getSteps().size());
        vertices = traversal11.toList();
        Assert.assertEquals(1, traversal11.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal12 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1).in().hasId(recordIdb1.toString(), recordIdc2.toString());
        Assert.assertEquals(3, traversal12.getSteps().size());
        vertices = traversal12.toList();
        Assert.assertEquals(1, traversal12.getSteps().size());
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testHasIdInJoin() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D");
        Vertex d3 = this.sqlgGraph.addVertex(T.label, "D");
        Vertex d4 = this.sqlgGraph.addVertex(T.label, "D");
        b1.addEdge("ab", a1);
        b2.addEdge("ab", a2);
        b3.addEdge("ab", a3);
        b4.addEdge("ab", a4);
        c1.addEdge("ac", b1);
        c1.addEdge("ac", b2);
        c1.addEdge("ac", b3);
        c1.addEdge("ac", b4);
        d1.addEdge("ac", c1);
        d2.addEdge("ac", c2);
        d3.addEdge("ac", c3);
        d4.addEdge("ac", c4);
        this.sqlgGraph.tx().commit();

        testHasIdInJoin_assert(this.sqlgGraph, a1, a2, a3, a4);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testHasIdInJoin_assert(this.sqlgGraph, a1, a2, a3, a4);
        }
    }

    private void testHasIdInJoin_assert(SqlgGraph sqlgGraph, Vertex a1, Vertex a2, Vertex a3, Vertex a4) {
        long start = sqlgGraph.getSqlDialect().getPrimaryKeyStartValue();
        RecordId recordIda1 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "A"), start);
        RecordId recordIda2 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "A"), start + 1L);
        RecordId recordIda3 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "A"), start + 2L);
        RecordId recordIda4 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "A"), start + 3L);
        RecordId recordIdb1 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "B"), start);
        RecordId recordIdb2 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "B"), start + 1L);
        RecordId recordIdb3 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "B"), start + 2L);
        RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "B"), start + 3L);
        RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "C"), start);
        RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "C"), start + 1L);
        RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "C"), start + 2L);
        RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "C"), start + 3L);

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1, recordIda2, recordIda3, recordIda4).in().hasId(recordIdb1, recordIdb2, recordIdb3);
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(3, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1.toString(), recordIda2.toString(), recordIda3.toString(), recordIda4.toString()).in()
                .hasId(recordIdb1.toString(), recordIdb2.toString(), recordIdb3.toString());
        Assert.assertEquals(3, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(3, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(a1, a2, a3, a4).in()
                .hasId(recordIdb1.toString(), recordIdb2.toString(), recordIdb3.toString());
        Assert.assertEquals(3, traversal2.getSteps().size());
        vertices = traversal2.toList();
        Assert.assertEquals(1, traversal2.getSteps().size());
        Assert.assertEquals(3, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(a1, a2, a3, a4).in()
                .hasId(recordIdb1.toString(), recordIdb2.toString(), recordIdb3.toString());
        Assert.assertEquals(3, traversal3.getSteps().size());
        vertices = traversal3.toList();
        Assert.assertEquals(1, traversal3.getSteps().size());
        Assert.assertEquals(3, vertices.size());
    }

    @Test
    public void testHasIdOutJoin() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D");
        Vertex d3 = this.sqlgGraph.addVertex(T.label, "D");
        Vertex d4 = this.sqlgGraph.addVertex(T.label, "D");
        a1.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        a3.addEdge("ab", b3);
        a4.addEdge("ab", b4);
        a1.addEdge("ac", c1);
        a1.addEdge("ac", c2);
        a1.addEdge("ac", c3);
        a1.addEdge("ac", c4);
        c1.addEdge("ac", d1);
        c2.addEdge("ac", d2);
        c3.addEdge("ac", d3);
        c4.addEdge("ac", d4);
        this.sqlgGraph.tx().commit();

        testHasIdOutJoin_assert(this.sqlgGraph, a1, a2, a3, a4);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testHasIdOutJoin_assert(this.sqlgGraph1, a1, a2, a3, a4);
        }
    }

    private void testHasIdOutJoin_assert(SqlgGraph sqlgGraph, Vertex a1, Vertex a2, Vertex a3, Vertex a4) {
        long start = sqlgGraph.getSqlDialect().getPrimaryKeyStartValue();
        RecordId recordIda1 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "A"), start);
        RecordId recordIda2 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "A"), start + 1L);
        RecordId recordIda3 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "A"), start + 2L);
        RecordId recordIda4 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "A"), start + 3L);
        RecordId recordIdb1 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "B"), start);
        RecordId recordIdb2 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "B"), start + 1L);
        RecordId recordIdb3 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "B"), start + 2L);
        RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "B"), start + 3L);
        RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "C"), start);
        RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "C"), start + 1L);
        RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "C"), start + 2L);
        RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "C"), start + 3L);

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1, recordIda2, recordIda3, recordIda4).out().hasId(recordIdb1, recordIdb2, recordIdb3);
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(3, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1.toString(), recordIda2.toString(), recordIda3.toString(), recordIda4.toString()).out()
                .hasId(recordIdb1.toString(), recordIdb2.toString(), recordIdb3.toString());
        Assert.assertEquals(3, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(3, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(a1, a2, a3, a4).out()
                .hasId(recordIdb1.toString(), recordIdb2.toString(), recordIdb3.toString());
        Assert.assertEquals(3, traversal2.getSteps().size());
        vertices = traversal2.toList();
        Assert.assertEquals(1, traversal2.getSteps().size());
        Assert.assertEquals(3, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(a1, a2, a3, a4).out()
                .hasId(recordIdb1.toString(), recordIdb2.toString(), recordIdb3.toString());
        Assert.assertEquals(3, traversal3.getSteps().size());
        vertices = traversal3.toList();
        Assert.assertEquals(1, traversal3.getSteps().size());
        Assert.assertEquals(3, vertices.size());
    }

    @Test
    public void testHasIdOut() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        a1.addEdge("ab", b4);
        a1.addEdge("ac", c1);
        a1.addEdge("ac", c2);
        a1.addEdge("ac", c3);
        a1.addEdge("ac", c4);
        this.sqlgGraph.tx().commit();

        testHasIdOut_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testHasIdOut_assert(this.sqlgGraph1);
        }

    }

    private void testHasIdOut_assert(SqlgGraph sqlgGraph) {
        long start = sqlgGraph.getSqlDialect().getPrimaryKeyStartValue();
        RecordId recordIda1 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "A"), start);
        RecordId recordIda2 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "A"), start + 1L);
        RecordId recordIda3 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "A"), start + 2L);
        RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "A"), start + 3L);
        RecordId recordIdb1 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "B"), start);
        RecordId recordIdb2 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "B"), start + 1L);
        RecordId recordIdb3 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "B"), start + 2L);
        RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "B"), start + 3L);
        RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "C"), start);
        RecordId recordIdc2 = RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "C"), start + 1L);
        RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "C"), start + 2L);
        RecordId.from(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "C"), start + 3L);

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1).hasLabel("A");
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1).has(T.id, P.within(recordIda2, recordIdb1));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(0, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V().has(T.id, P.within(recordIda1, recordIda2, recordIdb1));
        Assert.assertEquals(2, traversal2.getSteps().size());
        vertices = traversal2.toList();
        Assert.assertEquals(1, traversal2.getSteps().size());
        Assert.assertEquals(3, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1, recordIda2, recordIda3, recordIdb1);
        Assert.assertEquals(1, traversal3.getSteps().size());
        vertices = traversal3.toList();
        Assert.assertEquals(1, traversal3.getSteps().size());
        Assert.assertEquals(4, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal4 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V().has(T.id, P.within(recordIda1));
        Assert.assertEquals(2, traversal4.getSteps().size());
        vertices = traversal4.toList();
        Assert.assertEquals(1, traversal4.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal5 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1).out().hasId(recordIdb1);
        Assert.assertEquals(3, traversal5.getSteps().size());
        vertices = traversal5.toList();
        Assert.assertEquals(1, traversal5.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal6 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1, recordIda2, recordIda3).out().hasId(recordIdb1, recordIdb2, recordIdb3);
        Assert.assertEquals(3, traversal6.getSteps().size());
        vertices = traversal6.toList();
        Assert.assertEquals(1, traversal6.getSteps().size());
        Assert.assertEquals(3, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal7 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1).out().hasId(recordIda1);
        Assert.assertEquals(3, traversal7.getSteps().size());
        vertices = traversal7.toList();
        Assert.assertEquals(1, traversal7.getSteps().size());
        Assert.assertEquals(0, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal8 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1).out().hasId(recordIdb1);
        Assert.assertEquals(3, traversal8.getSteps().size());
        vertices = traversal8.toList();
        Assert.assertEquals(1, traversal8.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal9 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1).out().hasId(recordIdb1, recordIdb2);
        Assert.assertEquals(3, traversal9.getSteps().size());
        vertices = traversal9.toList();
        Assert.assertEquals(1, traversal9.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal10 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1).out().hasId(recordIdb1.toString());
        Assert.assertEquals(3, traversal10.getSteps().size());
        vertices = traversal10.toList();
        Assert.assertEquals(1, traversal10.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal11 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1).out().hasId(recordIdb1.toString(), recordIdb2.toString());
        Assert.assertEquals(3, traversal11.getSteps().size());
        vertices = traversal11.toList();
        Assert.assertEquals(1, traversal11.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal12 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(recordIda1).out().hasId(recordIdb1.toString(), recordIdc2.toString());
        Assert.assertEquals(3, traversal12.getSteps().size());
        vertices = traversal12.toList();
        Assert.assertEquals(1, traversal12.getSteps().size());
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void g_V_asXaX_out_asXbX_selectXa_bX_byXnameX() {
        Graph g = this.sqlgGraph;
        loadModern(this.sqlgGraph);
        assertModernGraph(g, true, false);
        DefaultGraphTraversal<Vertex, Map<String, String>> traversal = (DefaultGraphTraversal<Vertex, Map<String, String>>) g.traversal()
                .V().as("a").out().aggregate("x").as("b").<String>select("a", "b").by("name");
        Assert.assertEquals(4, traversal.getSteps().size());
        printTraversalForm(traversal);
        Assert.assertEquals(3, traversal.getSteps().size());
        final List<Map<String, String>> expected = makeMapList(2,
                "a", "marko", "b", "lop",
                "a", "marko", "b", "vadas",
                "a", "marko", "b", "josh",
                "a", "josh", "b", "ripple",
                "a", "josh", "b", "lop",
                "a", "peter", "b", "lop");
        checkResults(expected, traversal);
    }


    @Test
    public void g_VX1AsStringX_out_hasXid_2AsStringX() {
        Graph g = this.sqlgGraph;
        loadModern(this.sqlgGraph);
        assertModernGraph(g, true, false);
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal().V(convertToVertexId("marko")).out().hasId(convertToVertexId("vadas"));
        Assert.assertEquals(3, traversal.getSteps().size());
        printTraversalForm(traversal);
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertThat(traversal.hasNext(), CoreMatchers.is(true));
        Assert.assertEquals(convertToVertexId("vadas"), traversal.next().id());
        Assert.assertThat(traversal.hasNext(), CoreMatchers.is(false));
    }

    @Test
    public void g_VX4X_out_asXhereX_hasXlang_javaX_selectXhereX_name() {
        Graph g = this.sqlgGraph;
        loadModern(this.sqlgGraph);
        assertModernGraph(g, true, false);
        Object vertexId = convertToVertexId("josh");
        DefaultGraphTraversal<Vertex, String> traversal = (DefaultGraphTraversal<Vertex, String>) g.traversal().V(vertexId)
                .out().as("here")
                .has("lang", "java")
                .select("here").<String>values("name");
        Assert.assertEquals(5, traversal.getSteps().size());
        printTraversalForm(traversal);
        if (traversal.getSteps().size() != 3 && traversal.getSteps().size() != 4) {
            Assert.fail("expected 3 or 4 found " + traversal.getSteps().size());
        }
        int counter = 0;
        final Set<String> names = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            names.add(traversal.next());
        }
        Assert.assertEquals(2, counter);
        Assert.assertEquals(2, names.size());
        Assert.assertTrue(names.contains("ripple"));
        Assert.assertTrue(names.contains("lop"));
    }

    @Test
    public void g_V_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_bothXknowsX_bothXknowsX_asXdX_whereXc__notXeqXaX_orXeqXdXXXX_selectXa_b_c_dX() {
        Graph g = this.sqlgGraph;
        loadModern(this.sqlgGraph);
        assertModernGraph(g, true, false);
        DefaultGraphTraversal<Vertex, Map<String, Object>> traversal = (DefaultGraphTraversal<Vertex, Map<String, Object>>) this.sqlgGraph.traversal()
                .V().as("a")
                .out("created").as("b")
                .in("created").as("c")
                .both("knows")
                .both("knows").as("d")
                .where("c", P.not(P.eq("a").or(P.eq("d"))))
                .select("a", "b", "c", "d");
        Assert.assertEquals(7, traversal.getSteps().size());
        printTraversalForm(traversal);
        Assert.assertEquals(3, traversal.getSteps().size());
        checkResults(makeMapList(4,
                "a", convertToVertex(this.sqlgGraph, "marko"), "b", convertToVertex(this.sqlgGraph, "lop"), "c", convertToVertex(this.sqlgGraph, "josh"), "d", convertToVertex(this.sqlgGraph, "vadas"),
                "a", convertToVertex(this.sqlgGraph, "peter"), "b", convertToVertex(this.sqlgGraph, "lop"), "c", convertToVertex(this.sqlgGraph, "josh"), "d", convertToVertex(this.sqlgGraph, "vadas")), traversal);
    }

    @SuppressWarnings("unchecked")
    protected static <T> void checkResults(final List<T> expectedResults, final Traversal<?, T> traversal) {
        final List<T> results = traversal.toList();
        Assert.assertFalse(traversal.hasNext());
        if (expectedResults.size() != results.size()) {
            System.err.println("Expected results: " + expectedResults);
            System.err.println("Actual results:   " + results);
            Assert.assertEquals("Checking result size", expectedResults.size(), results.size());
        }

        for (T t : results) {
            if (t instanceof Map) {
                Assert.assertTrue("Checking map result existence: " + t, expectedResults.stream().filter(e -> e instanceof Map).anyMatch(e -> checkMap((Map) e, (Map) t)));
            } else {
                Assert.assertTrue("Checking result existence: " + t, expectedResults.contains(t));
            }
        }
        final Map<T, Long> expectedResultsCount = new HashMap<>();
        final Map<T, Long> resultsCount = new HashMap<>();
        Assert.assertEquals("Checking indexing is equivalent", expectedResultsCount.size(), resultsCount.size());
        expectedResults.forEach(t -> MapHelper.incr(expectedResultsCount, t, 1L));
        results.forEach(t -> MapHelper.incr(resultsCount, t, 1L));
        expectedResultsCount.forEach((k, v) -> Assert.assertEquals("Checking result group counts", v, resultsCount.get(k)));
        Assert.assertFalse(traversal.hasNext());
    }

    @SuppressWarnings("Duplicates")
    private static <A, B> boolean checkMap(final Map<A, B> expectedMap, final Map<A, B> actualMap) {
        final List<Map.Entry<A, B>> actualList = actualMap.entrySet().stream().sorted(Comparator.comparing(a -> a.getKey().toString())).collect(Collectors.toList());
        final List<Map.Entry<A, B>> expectedList = expectedMap.entrySet().stream().sorted(Comparator.comparing(a -> a.getKey().toString())).collect(Collectors.toList());

        if (expectedList.size() > actualList.size()) {
            return false;
        } else if (actualList.size() > expectedList.size()) {
            return false;
        }

        for (int i = 0; i < actualList.size(); i++) {
            if (!actualList.get(i).getKey().equals(expectedList.get(i).getKey())) {
                return false;
            }
            if (!actualList.get(i).getValue().equals(expectedList.get(i).getValue())) {
                return false;
            }
        }
        return true;
    }

    @Test
    public void g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_rangeXlocal_1_2X() {
        loadModern();
        Graph g = this.sqlgGraph;
        assertModernGraph(g, true, false);
        DefaultGraphTraversal<Vertex, String> traversal = (DefaultGraphTraversal<Vertex, String>) g.traversal()
                .V().as("a")
                .out().as("a")
                .out().as("a")
                .<List<String>>select(Pop.all, "a")
                .by(__.unfold().values("name").fold())
                .<String>range(Scope.local, 1, 2);
        Assert.assertEquals(5, traversal.getSteps().size());
        int counter = 0;
        while (traversal.hasNext()) {
            final String s = traversal.next();
            Assert.assertEquals("josh", s);
            counter++;
        }
        Assert.assertEquals(2, counter);
        Assert.assertEquals(3, traversal.getSteps().size());
    }

    @Test
    public void g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_rangeXlocal_1_2X_Simple() {
        loadModern();
        Graph g = this.sqlgGraph;
        assertModernGraph(g, true, false);
        DefaultGraphTraversal<Vertex, List<Vertex>> traversal = (DefaultGraphTraversal<Vertex, List<Vertex>>) g.traversal()
                .V().as("a")
                .out().as("a")
                .out().as("a")
                .<List<Vertex>>select(Pop.all, "a");
        Assert.assertEquals(4, traversal.getSteps().size());
        printTraversalForm(traversal);
        Assert.assertEquals(2, traversal.getSteps().size());
        int counter = 0;
        while (traversal.hasNext()) {
            final List<Vertex> s = traversal.next();
            Assert.assertEquals(3, s.size());
            System.out.println(s);
            counter++;
        }
        Assert.assertEquals(2, counter);
    }

    @Test
    public void g_V_hasXinXcreatedX_count_isXgte_2XX_valuesXnameX() {
        Graph g = this.sqlgGraph;
        loadModern(this.sqlgGraph);
        assertModernGraph(g, true, false);
        DefaultGraphTraversal<Vertex, String> traversal = (DefaultGraphTraversal<Vertex, String>) this.sqlgGraph.traversal()
                .V()
                .where(
                        __.in("created")
                                .count()
                                .is(
                                        P.gte(2L)
                                )
                )
                .<String>values("name");
        Assert.assertEquals(3, traversal.getSteps().size());
        printTraversalForm(traversal);
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.hasNext());
        Assert.assertEquals("lop", traversal.next());
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void g_VX1X_out_hasXid_2X() {
        Graph g = this.sqlgGraph;
        loadModern(this.sqlgGraph);
        assertModernGraph(g, true, false);
        Object marko = convertToVertexId("marko");
        Object vadas = convertToVertexId("vadas");
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal().V(marko).out().hasId(vadas);
        Assert.assertEquals(3, traversal.getSteps().size());
        printTraversalForm(traversal);
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertThat(traversal.hasNext(), CoreMatchers.is(true));
        Assert.assertEquals(convertToVertexId("vadas"), traversal.next().id());
        Assert.assertThat(traversal.hasNext(), CoreMatchers.is(false));
    }

    @Test
    public void testHasLabelOut() {
        SqlgVertex a1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        SqlgVertex b1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("outB", b1);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal().V().both().has(T.label, "B");
        Assert.assertEquals(3, traversal.getSteps().size());
        printTraversalForm(traversal);
        Assert.assertEquals(1, traversal.getSteps().size());
        List<Vertex> softwares = traversal.toList();
        Assert.assertEquals(1, softwares.size());
        for (Vertex software : softwares) {
            if (!software.label().equals("B")) {
                Assert.fail("expected label B found " + software.label());
            }
        }
    }

    @Test
    public void testSingleCompileWithHasLabelIn() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "b4");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("outB", b1);
        b1.addEdge("outC", c1);
        a2.addEdge("outB", b2);
        b2.addEdge("outC", c1);
        a3.addEdge("outB", b3);
        b3.addEdge("outC", c1);
        d1.addEdge("outB", b4);
        b4.addEdge("outC", c1);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(4, vertexTraversal(this.sqlgGraph, c1).in().in().count().next().intValue());
        Assert.assertEquals(3, vertexTraversal(this.sqlgGraph, c1).in().in().has(T.label, "A").count().next().intValue());
    }

    @Test
    public void testOutHasOutHas() {
        SqlgVertex a1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        SqlgVertex b1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        SqlgVertex b2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        SqlgVertex b3 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("outB", b1);
        a1.addEdge("outB", b2);
        a1.addEdge("outB", b3);
        SqlgVertex c1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        SqlgVertex c2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        SqlgVertex c3 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        SqlgVertex c4 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c4");
        SqlgVertex c5 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c5");
        SqlgVertex c6 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c6");
        SqlgVertex c7 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c7");
        SqlgVertex c8 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c8");
        SqlgVertex c9 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c9");
        b1.addEdge("outC", c1);
        b1.addEdge("outC", c2);
        b1.addEdge("outC", c3);
        b2.addEdge("outC", c4);
        b2.addEdge("outC", c5);
        b2.addEdge("outC", c6);
        b3.addEdge("outC", c7);
        b3.addEdge("outC", c8);
        b3.addEdge("outC", c9);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Long> traversal = (DefaultGraphTraversal<Vertex, Long>) vertexTraversal(this.sqlgGraph, a1)
                .out().has("name", "b1").out().has("name", "c1").count();
        Assert.assertEquals(6, traversal.getSteps().size());
        Assert.assertEquals(1, traversal.next().intValue());
        Assert.assertEquals(3, traversal.getSteps().size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) vertexTraversal(this.sqlgGraph, a1)
                .out().has("name", "b1").out().has("name", "c1");
        Assert.assertEquals(5, traversal1.getSteps().size());
        Assert.assertEquals(c1, traversal1.next());
        Assert.assertEquals(1, traversal1.getSteps().size());

        DefaultGraphTraversal<Vertex, Long> traversal2 = (DefaultGraphTraversal<Vertex, Long>) vertexTraversal(this.sqlgGraph, a1)
                .out().has("name", "b2").out().has("name", "c5").count();
        Assert.assertEquals(6, traversal2.getSteps().size());
        Assert.assertEquals(1, traversal2.next().intValue());
        Assert.assertEquals(3, traversal2.getSteps().size());

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) vertexTraversal(this.sqlgGraph, a1)
                .out().has("name", "b2").out().has("name", "c5");
        Assert.assertEquals(5, traversal3.getSteps().size());
        Assert.assertEquals(c5, traversal3.next());
        Assert.assertEquals(1, traversal3.getSteps().size());
    }

    @Test
    public void testOutHasOutHasNotParsed() {
        SqlgVertex a1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        SqlgVertex b1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        SqlgVertex b2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        SqlgVertex b3 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("outB", b1);
        a1.addEdge("outB", b2);
        a1.addEdge("outB", b3);
        SqlgVertex c1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        SqlgVertex c2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        SqlgVertex c3 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        SqlgVertex c4 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c4");
        SqlgVertex c5 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c5");
        SqlgVertex c6 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c6");
        SqlgVertex c7 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c7");
        SqlgVertex c8 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c8");
        SqlgVertex c9 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c9");
        b1.addEdge("outC", c1);
        b1.addEdge("outC", c2);
        b1.addEdge("outC", c3);
        b2.addEdge("outC", c4);
        b2.addEdge("outC", c5);
        b2.addEdge("outC", c6);
        b3.addEdge("outC", c7);
        b3.addEdge("outC", c8);
        b3.addEdge("outC", c9);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Long> traversal = (DefaultGraphTraversal<Vertex, Long>) vertexTraversal(this.sqlgGraph, a1)
                .out().has("name", "b1").out().has("name", "c1").count();
        Assert.assertEquals(6, traversal.getSteps().size());
        Assert.assertEquals(1, traversal.next().intValue());
        Assert.assertEquals(3, traversal.getSteps().size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) vertexTraversal(this.sqlgGraph, a1)
                .out().has("name", "b1").out().has("name", "c1");
        Assert.assertEquals(5, traversal1.getSteps().size());
        Assert.assertEquals(c1, traversal1.next());
        Assert.assertEquals(1, traversal1.getSteps().size());

        DefaultGraphTraversal<Vertex, Long> traversal2 = (DefaultGraphTraversal<Vertex, Long>) vertexTraversal(this.sqlgGraph, a1)
                .out().has("name", "b2").out().has("name", "c5").count();
        Assert.assertEquals(6, traversal2.getSteps().size());
        Assert.assertEquals(1, traversal2.next().intValue());
        Assert.assertEquals(3, traversal2.getSteps().size());

        DefaultGraphTraversal<Vertex, Long> traversal3 = (DefaultGraphTraversal<Vertex, Long>) vertexTraversal(this.sqlgGraph, a1)
                .out().has("name", "b2").has("name", "b2").out().has("name", P.within(Arrays.asList("c5", "c6"))).count();
        Assert.assertEquals(6, traversal3.getSteps().size());
        Assert.assertEquals(2, traversal3.next().intValue());
        Assert.assertEquals(3, traversal3.getSteps().size());

        DefaultGraphTraversal<Vertex, Long> traversal4 = (DefaultGraphTraversal<Vertex, Long>) vertexTraversal(this.sqlgGraph, a1)
                .out().has("name", "b2").has("name", "b2").out().has("name", P.eq("c5")).count();
        Assert.assertEquals(6, traversal4.getSteps().size());
        Assert.assertEquals(1, traversal4.next().intValue());
        Assert.assertEquals(3, traversal4.getSteps().size());
    }

    @Test
    public void testInOut() {
        Vertex v1 = sqlgGraph.addVertex();
        Vertex v2 = sqlgGraph.addVertex();
        Vertex v3 = sqlgGraph.addVertex();
        Vertex v4 = sqlgGraph.addVertex();
        sqlgGraph.addVertex();
        Edge e1 = v1.addEdge("label1", v2);
        Edge e2 = v2.addEdge("label2", v3);
        v3.addEdge("label3", v4);
        sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Long> traversal = (DefaultGraphTraversal<Vertex, Long>) vertexTraversal(this.sqlgGraph, v2).inE().count();
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertEquals(1, traversal.next(), 1);
        Assert.assertEquals(3, traversal.getSteps().size());

        DefaultGraphTraversal<Vertex, Edge> traversal1 = (DefaultGraphTraversal<Vertex, Edge>) vertexTraversal(this.sqlgGraph, v2).inE();
        Assert.assertEquals(2, traversal1.getSteps().size());
        Assert.assertEquals(e1, traversal1.next());
        Assert.assertEquals(1, traversal1.getSteps().size());

        DefaultGraphTraversal<Edge, Long> traversal2 = (DefaultGraphTraversal<Edge, Long>) edgeTraversal(this.sqlgGraph, e1).inV().count();
        Assert.assertEquals(3, traversal2.getSteps().size());
        Assert.assertEquals(1L, traversal2.next(), 0);
        Assert.assertEquals(3, traversal2.getSteps().size());

        DefaultGraphTraversal<Edge, Vertex> traversal3 = (DefaultGraphTraversal<Edge, Vertex>) edgeTraversal(this.sqlgGraph, e1).inV();
        Assert.assertEquals(2, traversal3.getSteps().size());
        Assert.assertEquals(v2, traversal3.next());
        Assert.assertEquals(1, traversal3.getSteps().size());

        DefaultGraphTraversal<Edge, Long> traversal4 = (DefaultGraphTraversal<Edge, Long>) edgeTraversal(this.sqlgGraph, e1).outV().count();
        Assert.assertEquals(3, traversal4.getSteps().size());
        Assert.assertEquals(1L, traversal4.next(), 0);
        Assert.assertEquals(3, traversal4.getSteps().size());

        DefaultGraphTraversal<Edge, Long> traversal5 = (DefaultGraphTraversal<Edge, Long>) edgeTraversal(this.sqlgGraph, e1).outV().inE().count();
        Assert.assertEquals(4, traversal5.getSteps().size());
        Assert.assertEquals(0L, traversal5.next(), 0);
        Assert.assertEquals(3, traversal5.getSteps().size());

        DefaultGraphTraversal<Edge, Long> traversal6 = (DefaultGraphTraversal<Edge, Long>) edgeTraversal(this.sqlgGraph, e2).inV().count();
        Assert.assertEquals(3, traversal6.getSteps().size());
        Assert.assertEquals(1L, traversal6.next(), 0);
        Assert.assertEquals(3, traversal6.getSteps().size());

        DefaultGraphTraversal<Edge, Vertex> traversal7 = (DefaultGraphTraversal<Edge, Vertex>) edgeTraversal(this.sqlgGraph, e2).inV();
        Assert.assertEquals(2, traversal7.getSteps().size());
        Assert.assertEquals(v3, traversal7.next());
        Assert.assertEquals(1, traversal7.getSteps().size());
    }

    @Test
    public void testVertexOutWithHas() {
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex bmw1 = this.sqlgGraph.addVertex(T.label, "Car", "name", "bmw", "cc", 600);
        Vertex bmw2 = this.sqlgGraph.addVertex(T.label, "Car", "name", "bmw", "cc", 800);
        Vertex ktm1 = this.sqlgGraph.addVertex(T.label, "Bike", "name", "ktm", "cc", 200);
        Vertex ktm2 = this.sqlgGraph.addVertex(T.label, "Bike", "name", "ktm", "cc", 200);
        Vertex ktm3 = this.sqlgGraph.addVertex(T.label, "Bike", "name", "ktm", "cc", 400);
        marko.addEdge("drives", bmw1);
        marko.addEdge("drives", bmw2);
        marko.addEdge("drives", ktm1);
        marko.addEdge("drives", ktm2);
        marko.addEdge("drives", ktm3);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) vertexTraversal(this.sqlgGraph, marko)
                .out("drives").<Vertex>has("name", "bmw");
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Vertex> drivesBmw = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(2L, drivesBmw.size(), 0);

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) vertexTraversal(this.sqlgGraph, marko)
                .out("drives").<Vertex>has("name", "ktm");
        Assert.assertEquals(3, traversal1.getSteps().size());
        List<Vertex> drivesKtm = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(3L, drivesKtm.size(), 0);

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) vertexTraversal(this.sqlgGraph, marko)
                .out("drives").<Vertex>has("cc", 600);
        Assert.assertEquals(3, traversal2.getSteps().size());
        List<Vertex> cc600 = traversal2.toList();
        Assert.assertEquals(1, traversal2.getSteps().size());
        Assert.assertEquals(1L, cc600.size(), 0);

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) vertexTraversal(this.sqlgGraph, marko)
                .out("drives").<Vertex>has("cc", 800);
        Assert.assertEquals(3, traversal3.getSteps().size());
        List<Vertex> cc800 = traversal3.toList();
        Assert.assertEquals(1, traversal3.getSteps().size());
        Assert.assertEquals(1L, cc800.size(), 0);

        DefaultGraphTraversal<Vertex, Vertex> traversal4 = (DefaultGraphTraversal<Vertex, Vertex>) vertexTraversal(this.sqlgGraph, marko)
                .out("drives").<Vertex>has("cc", 200);
        Assert.assertEquals(3, traversal4.getSteps().size());
        List<Vertex> cc200 = traversal4.toList();
        Assert.assertEquals(1, traversal4.getSteps().size());
        Assert.assertEquals(2L, cc200.size(), 0);

        DefaultGraphTraversal<Vertex, Vertex> traversal5 = (DefaultGraphTraversal<Vertex, Vertex>) vertexTraversal(this.sqlgGraph, marko)
                .out("drives").<Vertex>has("cc", 400);
        Assert.assertEquals(3, traversal5.getSteps().size());
        List<Vertex> cc400 = traversal5.toList();
        Assert.assertEquals(1, traversal5.getSteps().size());
        Assert.assertEquals(1L, cc400.size(), 0);
    }

    @Test
    public void testg_EX11X_outV_outE_hasXid_10AsStringX() {
        Graph g = this.sqlgGraph;
        loadModern(this.sqlgGraph);
        assertModernGraph(g, true, false);
        final Object edgeId11 = convertToEdgeId(this.sqlgGraph, "josh", "created", "lop");
        final Object edgeId10 = convertToEdgeId(this.sqlgGraph, "josh", "created", "ripple");
        DefaultGraphTraversal<Edge, Edge> traversal = (DefaultGraphTraversal<Edge, Edge>) g.traversal().E(edgeId11.toString())
                .outV().outE().has(T.id, edgeId10.toString());
        Assert.assertEquals(4, traversal.getSteps().size());
        printTraversalForm(traversal);
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.hasNext());
        final Edge e = traversal.next();
        Assert.assertEquals(edgeId10.toString(), e.id().toString());
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void g_V_out_outE_inV_inE_inV_both_name() {
        Graph g = this.sqlgGraph;
        loadModern(this.sqlgGraph);
        assertModernGraph(g, true, false);

        Object id = convertToVertexId(g, "marko");
        DefaultGraphTraversal<Vertex, String> traversal = (DefaultGraphTraversal<Vertex, String>) g.traversal().V(id).out().outE().inV().inE().inV().both().<String>values("name");
        Assert.assertEquals(8, traversal.getSteps().size());
        printTraversalForm(traversal);
        Assert.assertEquals(2, traversal.getSteps().size());
        int counter = 0;
        final Map<String, Integer> counts = new HashMap<>();
        while (traversal.hasNext()) {
            final String key = traversal.next();
            final int previousCount = counts.getOrDefault(key, 0);
            counts.put(key, previousCount + 1);
            counter++;
        }
        Assert.assertEquals(3, counts.size());
        Assert.assertEquals(4, counts.get("josh").intValue());
        Assert.assertEquals(3, counts.get("marko").intValue());
        Assert.assertEquals(3, counts.get("peter").intValue());

        Assert.assertEquals(10, counter);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testHasWithStringIds() {
        Graph g = this.sqlgGraph;
        loadModern(this.sqlgGraph);
        assertModernGraph(g, true, false);
        String marko = convertToVertexId("marko").toString();
        String vadas = convertToVertexId("vadas").toString();
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) g.traversal().V(marko).out().hasId(vadas);
        Assert.assertEquals(3, traversal.getSteps().size());
        printTraversalForm(traversal);
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.hasNext());
        Assert.assertEquals(convertToVertexId("vadas"), traversal.next().id());
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void testHas() {
        Graph g = this.sqlgGraph;
        loadModern(this.sqlgGraph);
        assertModernGraph(g, true, false);
        final Object id2 = convertToVertexId("vadas");
        final Object id3 = convertToVertexId("lop");
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) get_g_VX1X_out_hasIdX2_3X(g.traversal(), convertToVertexId("marko"), id2.toString(), id3.toString());
        Assert.assertEquals(3, traversal.getSteps().size());
        assert_g_VX1X_out_hasXid_2_3X(id2, id3, traversal);
        Assert.assertEquals(1, traversal.getSteps().size());
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void g_VX1X_out_hasXid_2AsString_3AsStringX() {
        Graph g = this.sqlgGraph;
        loadModern(this.sqlgGraph);
        assertModernGraph(g, true, false);
        final Object id2 = convertToVertexId("vadas");
        final Object id3 = convertToVertexId("lop");
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) get_g_VX1X_out_hasIdX2_3X(g.traversal(), convertToVertexId("marko"), id2.toString(), id3.toString());
        Assert.assertEquals(3, traversal.getSteps().size());
        assert_g_VX1X_out_hasXid_2_3X(id2, id3, traversal);
        Assert.assertEquals(1, traversal.getSteps().size());
    }

    @Test
    public void testX() {
        Graph g = this.sqlgGraph;
        loadModern(this.sqlgGraph);
        assertModernGraph(g, true, false);
        final Object marko = convertToVertexId("marko");
        DefaultGraphTraversal<Vertex, Edge> traversal = (DefaultGraphTraversal<Vertex, Edge>) g.traversal().V(marko)
                .outE("knows").has("weight", 1.0d).as("here").inV().has("name", "josh").<Edge>select("here");
        Assert.assertEquals(6, traversal.getSteps().size());
        printTraversalForm(traversal);
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.hasNext());
        Assert.assertTrue(traversal.hasNext());
        final Edge edge = traversal.next();
        Assert.assertEquals("knows", edge.label());
        Assert.assertEquals(1.0d, edge.<Double>value("weight"), 0.00001d);
        Assert.assertFalse(traversal.hasNext());
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void g_VX1X_outE_hasXweight_inside_0_06X_inV() {
        Graph g = this.sqlgGraph;
        loadModern(this.sqlgGraph);
        assertModernGraph(g, true, false);
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) get_g_VX1X_outE_hasXweight_inside_0_06X_inV(g.traversal(), convertToVertexId("marko"));
        Assert.assertEquals(4, traversal.getSteps().size());
        printTraversalForm(traversal);
        Assert.assertEquals(1, traversal.getSteps().size());
        while (traversal.hasNext()) {
            Vertex vertex = traversal.next();
            Assert.assertTrue(vertex.value("name").equals("vadas") || vertex.value("name").equals("lop"));
        }
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testY() {
        Graph g = this.sqlgGraph;
        loadModern(this.sqlgGraph);
        assertModernGraph(g, true, false);
        Object marko = convertToVertexId(g, "marko");
        DefaultGraphTraversal<Vertex, String> traversal = (DefaultGraphTraversal<Vertex, String>) g.traversal().V(marko).outE("knows").bothV().<String>values("name");
        Assert.assertEquals(4, traversal.getSteps().size());
        printTraversalForm(traversal);
        Assert.assertEquals(2, traversal.getSteps().size());
        final List<String> names = traversal.toList();
        Assert.assertEquals(4, names.size());
        Assert.assertTrue(names.contains("marko"));
        Assert.assertTrue(names.contains("josh"));
        Assert.assertTrue(names.contains("vadas"));
        names.remove("marko");
        Assert.assertEquals(3, names.size());
        names.remove("marko");
        Assert.assertEquals(2, names.size());
        names.remove("josh");
        Assert.assertEquals(1, names.size());
        names.remove("vadas");
        Assert.assertEquals(0, names.size());
    }

    private Traversal<Vertex, Vertex> get_g_VX1X_outE_hasXweight_inside_0_06X_inV(GraphTraversalSource g, final Object v1Id) {
        return g.V(v1Id).outE().has("weight", P.inside(0.0d, 0.6d)).inV();
    }

    private Traversal<Vertex, Vertex> get_g_VX1X_out_hasIdX2_3X(GraphTraversalSource g, final Object v1Id, final Object v2Id, final Object v3Id) {
        return g.V(v1Id).out().hasId(v2Id, v3Id);
    }


    private void assert_g_VX1X_out_hasXid_2_3X(Object id2, Object id3, Traversal<Vertex, Vertex> traversal) {
        printTraversalForm(traversal);
        Assert.assertTrue(traversal.hasNext());
        Assert.assertThat(traversal.next().id(), CoreMatchers.anyOf(CoreMatchers.is(id2), CoreMatchers.is(id3)));
        Assert.assertThat(traversal.next().id(), CoreMatchers.anyOf(CoreMatchers.is(id2), CoreMatchers.is(id3)));
        Assert.assertFalse(traversal.hasNext());
    }

    public Object convertToEdgeId(final Graph graph, final String outVertexName, String edgeLabel, final String inVertexName) {
        return graph.traversal().V().has("name", outVertexName).outE(edgeLabel).as("e").inV().has("name", inVertexName).<Edge>select("e").next().id();
    }

}
