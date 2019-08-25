package org.umlg.sqlg.test.repeatstep;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.RepeatUnrollStrategy;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.core.AnyOf;
import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.step.barrier.SqlgRepeatStepBarrier;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static org.junit.Assert.assertFalse;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/06/06
 */
public class TestUnoptimizedRepeatStep extends BaseTest {

//    @Test
    //Takes very long need to investigate
    public void g_VX3X_repeatXbothX_createdXX_untilXloops_is_40XXemit_repeatXin_knowsXX_emit_loopsXisX1Xdedup_values() {
        loadModern();
        Object id = convertToVertexId("lop");
//        final Traversal<Vertex, String> traversal =  this.sqlgGraph.traversal().V(id)
//                .repeat(__.both("created"))
//                .until(loops().is(40))
//                .emit(
//                        __.repeat(__.in("knows"))
//                                .emit(loops().is(1)))
//                .dedup().values("name");


        final Traversal<Vertex, String> traversal =  this.sqlgGraph.traversal().V(id)
                .repeat(__.both("created"))
                .times(40)
                .dedup().values("name");

        printTraversalForm(traversal);
        checkResults(Arrays.asList("josh", "ripple", "lop"), traversal);
        assertFalse(traversal.hasNext());
    }

    //    @Test
    public void testRepeatStepPerformance() {
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 1000; i++) {
            Vertex a;
            if (i % 100 == 0) {
                a = this.sqlgGraph.addVertex(T.label, "A", "hubSite", true);
            } else {
                a = this.sqlgGraph.addVertex(T.label, "A", "hubSite", false);
            }
            for (int j = 0; j < 10; j++) {
                Vertex b = this.sqlgGraph.addVertex(T.label, "A", "hubSite", false);
                a.addEdge("link", b);
                for (int k = 0; k < 10; k++) {
                    Vertex c;
                    if (k == 5) {
                        c = this.sqlgGraph.addVertex(T.label, "A", "hubSite", false);
                    } else {
                        c = this.sqlgGraph.addVertex(T.label, "A", "hubSite", false);
                    }
                    b.addEdge("link", c);

                    for (int l = 0; l < 10; l++) {
                        Vertex d = this.sqlgGraph.addVertex(T.label, "A", "hubSite", true);
                        c.addEdge("link", d);
                    }
                }
            }
        }
        this.sqlgGraph.tx().commit();

        System.out.println("===========================");

        StopWatch stopWatch = new StopWatch();
        for (int i = 0; i < 1000; i++) {
            stopWatch.start();
            List<Path> vertices = this.sqlgGraph.traversal()
                    .V().has("hubSite", true)
                    .repeat(__.out())
                    .until(__.or(__.loops().is(P.gt(3)), __.has("hubSite", true)))
//                    .until(__.has("hubSite", true))
                    .path()
                    .toList();
            Assert.assertEquals(10000, vertices.size());
            stopWatch.stop();
            System.out.println(stopWatch.toString());
            stopWatch.reset();
        }
    }


    //    @Test
    public void testRepeatUtilFirstPerformance() {
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 10_000; i++) {
            Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
            Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
            Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
            Vertex c2 = this.sqlgGraph.addVertex(T.label, "C");
            Vertex x = this.sqlgGraph.addVertex(T.label, "X", "name", "hallo");
            a1.addEdge("ab", b1);
            a2.addEdge("ab", b2);
            b1.addEdge("bc", c1);
            b2.addEdge("bc", c2);

            b1.addEdge("bx", x);
            c2.addEdge("cx", x);
        }
        this.sqlgGraph.tx().commit();

        StopWatch stopWatch = new StopWatch();
        for (int i = 0; i < 1000; i++) {
            stopWatch.start();
            DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                    .V().hasLabel("A")
                    .repeat(__.out())
                    .until(__.out().has("name", "hallo"));
            List<Vertex> vertices = traversal.toList();
            Assert.assertEquals(20_000, vertices.size());
            stopWatch.stop();
            System.out.println(stopWatch.toString());
            stopWatch.reset();
        }
    }


    @Test
    public void g_V_valueMapXname_ageX_withXtokens_labelsX_byXunfoldX() {
        loadModern();


        final Traversal<Vertex, Map<Object, Object>> t = this.sqlgGraph.traversal().V()
                .valueMap("name", "age")
                .with(WithOptions.tokens, WithOptions.labels);
        List<Map<Object, Object>> result =  t.toList();
        System.out.println(result);

        final Traversal<Vertex, Map<Object, Object>> traversal = this.sqlgGraph.traversal().V()
                .valueMap("name", "age")
                .with(WithOptions.tokens, WithOptions.labels)
                .by(__.unfold());
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<Object, Object> values = traversal.next();
            final String name = (String) values.get("name");
            Assert.assertThat(values.containsKey(T.id), Matchers.is(false));
            if (name.equals("marko")) {
                Assert.assertEquals(3, values.size());
                Assert.assertEquals(29, values.get("age"));
                Assert.assertEquals("person", values.get(T.label));
            } else if (name.equals("josh")) {
                Assert.assertEquals(3, values.size());
                Assert.assertEquals(32, values.get("age"));
                Assert.assertEquals("person", values.get(T.label));
            } else if (name.equals("peter")) {
                Assert.assertEquals(3, values.size());
                Assert.assertEquals(35, values.get("age"));
                Assert.assertEquals("person", values.get(T.label));
            } else if (name.equals("vadas")) {
                Assert.assertEquals(3, values.size());
                Assert.assertEquals(27, values.get("age"));
                Assert.assertEquals("person", values.get(T.label));
            } else if (name.equals("lop")) {
                Assert.assertEquals(2, values.size());
                Assert.assertNull(values.get("lang"));
                Assert.assertEquals("software", values.get(T.label));
            } else if (name.equals("ripple")) {
                Assert.assertEquals(2, values.size());
                Assert.assertNull(values.get("lang"));
                Assert.assertEquals("software", values.get(T.label));
            } else {
                throw new IllegalStateException("It is not possible to reach here: " + values);
            }
        }
        Assert.assertEquals(6, counter);
    }

    @Test
    public void g_V_emitXhasXname_markoX_or_loops_isX2XX_repeatXoutX_valuesXnameX() {
        loadModern();
        final Traversal<Vertex, String> traversal = this.sqlgGraph.traversal().V()
                .emit(
                        __.has("name", "marko").or().loops().is(2)
                )
                .repeat(
                        __.out()
                )
                .values("name");
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "ripple", "lop"), traversal);
    }

    @Test
    public void g_VX6X_repeatXa_bothXcreatedX_simplePathX_emitXrepeatXb_bothXknowsXX_untilXloopsXbX_asXb_whereXloopsXaX_asXbX_hasXname_vadasXX_dedup_name() {
        loadModern();
        Object peterId = this.convertToVertexId("peter");
        Traversal<Vertex, String> traversal = this.sqlgGraph.traversal().V(peterId)
                .repeat("a", __.both("created").simplePath())
                .emit(
                        __.repeat("b", __.both("knows"))
                                .until(
                                        __.loops("b").as("c").where(__.loops("a").as("c"))
                                )
                                .has("name", "vadas")
                ).dedup().values("name");


        this.printTraversalForm(traversal);
        Assert.assertTrue(traversal.hasNext());
        String name = (String) traversal.next();
        Assert.assertEquals("josh", name);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void g_V_repeatXout_repeatXoutX_timesX1XX_timesX1X_limitX1X_path_by_name() {
        loadModern();
        Traversal<Vertex, Path> traversal_unrolled = this.sqlgGraph.traversal().V()
                .repeat(
                        __.out().repeat(
                                __.out()
                        ).times(1)
                ).times(1).limit(1L).path().by("name");
        Path pathOriginal = (Path) traversal_unrolled.next();
        Assert.assertFalse(traversal_unrolled.hasNext());
        Assert.assertEquals(3L, (long) pathOriginal.size());
        Assert.assertEquals("marko", pathOriginal.get(0));
        Assert.assertEquals("josh", pathOriginal.get(1));
        MatcherAssert.assertThat(pathOriginal.get(2), AnyOf.anyOf(IsEqual.equalTo("ripple"), IsEqual.equalTo("lop")));

        GraphTraversalSource g = this.sqlgGraph.traversal().withoutStrategies(new Class[]{RepeatUnrollStrategy.class});
        Traversal<Vertex, Path> traversal = g.V()
                .repeat(
                        __.out().repeat(
                                __.out()
                        ).times(1)
                ).times(1).limit(1L).path().by("name");


        this.printTraversalForm(traversal);
        Path path = (Path) traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertEquals(3L, (long) path.size());
        Assert.assertEquals("marko", path.get(0));
        Assert.assertEquals("josh", path.get(1));
        MatcherAssert.assertThat(path.get(2), AnyOf.anyOf(IsEqual.equalTo("ripple"), IsEqual.equalTo("lop")));
    }

    @Test
    public void testRepeatStepWithUntilLast() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex x = this.sqlgGraph.addVertex(T.label, "X", "name", "hallo");
        a1.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        b2.addEdge("bc", c2);

        b1.addEdge("bx", x);
        c2.addEdge("cx", x);

        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .repeat(__.out())
                .until(__.out().has("name", "hallo"));

        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(b1) || vertices.contains(c1));
    }

    @Test
    public void testRepeatStepWithUntilLastEmitFirst() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex x = this.sqlgGraph.addVertex(T.label, "X", "name", "hallo");
        a1.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        b2.addEdge("bc", c2);

        b1.addEdge("bx", x);
        c2.addEdge("cx", x);

        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .emit()
                .repeat(__.out())
                .until(__.out().has("name", "hallo"));

        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(5, vertices.size());
        Assert.assertTrue(vertices.contains(a1) && vertices.contains(a2) && vertices.contains(b1) || vertices.contains(b2) || vertices.contains(c2));
    }

    @Test
    public void testRepeatStepWithUntilLastEmitLast() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex x = this.sqlgGraph.addVertex(T.label, "X", "name", "hallo");
        a1.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        b2.addEdge("bc", c2);

        b1.addEdge("bx", x);
        c2.addEdge("cx", x);

        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .repeat(__.out())
                .emit()
                .until(__.out().has("name", "hallo"));

        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.contains(b1) || vertices.contains(b2) || vertices.contains(c2));
    }

    @Test
    public void testRepeatStepWithUntilFirst() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex x = this.sqlgGraph.addVertex(T.label, "X", "name", "hallo");
        a1.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        b2.addEdge("bc", c2);

        b1.addEdge("bx", x);
        c2.addEdge("cx", x);

        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .until(__.out().has("name", "hallo"))
                .repeat(__.out());

        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(b1) && vertices.contains(c2));
    }

    @Test
    public void testRepeatStepWithUntilFirstEmitFirst() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex x = this.sqlgGraph.addVertex(T.label, "X", "name", "hallo");
        a1.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        b2.addEdge("bc", c2);

        b1.addEdge("bx", x);
        c2.addEdge("cx", x);

        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .until(__.out().has("name", "hallo"))
                .emit()
                .repeat(__.out());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(5, vertices.size());
        Assert.assertTrue(vertices.contains(a1) && vertices.contains(a2) && vertices.contains(b1) && vertices.contains(b2) && vertices.contains(c2));
    }

    @Test
    public void testRepeatStepWithUntilFirstEmitLast() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex x = this.sqlgGraph.addVertex(T.label, "X", "name", "hallo");
        a1.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        b2.addEdge("bc", c2);

        b1.addEdge("bx", x);
        c2.addEdge("cx", x);

        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .until(__.out().has("name", "hallo"))
                .repeat(__.out())
                .emit();
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(5, vertices.size());
        Assert.assertTrue(vertices.contains(b1) && vertices.contains(b2) && vertices.contains(c2));
        int count = 0;
        for (Vertex vertex : vertices) {
            if (vertex.equals(b1)) {
                count++;
            }
        }
        Assert.assertEquals(2, count);
        count = 0;
        for (Vertex vertex : vertices) {
            if (vertex.equals(c2)) {
                count++;
            }
        }
        Assert.assertEquals(2, count);
    }

    @Test
    public void testRepeatStepWithLimit() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .until(__.has(T.label, "B"))
                .repeat(__.out())
                .limit(1);

        Assert.assertEquals(4, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(2) instanceof RepeatStep);
        Assert.assertTrue(traversal.getSteps().get(3) instanceof RangeGlobalStep);
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgRepeatStepBarrier);
        Assert.assertEquals(1, vertices.size());
        Assert.assertTrue(vertices.contains(b1) || vertices.contains(b2));

        vertices = this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .repeat(__.out())
                .until(__.has(T.label, "B"))
                .limit(1)
                .toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertTrue(vertices.contains(b1) || vertices.contains(b2));

    }

    @Test
    public void testHubSites() {
        Vertex a0 = this.sqlgGraph.addVertex(T.label, "A", "hubSite", true);

        Vertex a10 = this.sqlgGraph.addVertex(T.label, "A", "hubSite", false);
        Vertex a11 = this.sqlgGraph.addVertex(T.label, "A", "hubSite", false);
        Vertex a12 = this.sqlgGraph.addVertex(T.label, "A", "hubSite", false);
        Vertex a13 = this.sqlgGraph.addVertex(T.label, "A", "hubSite", false);
        Vertex a14 = this.sqlgGraph.addVertex(T.label, "A", "hubSite", true);
        a0.addEdge("a", a10);
        a0.addEdge("a", a11);
        a0.addEdge("a", a13);
        a0.addEdge("a", a14);

        Vertex a20 = this.sqlgGraph.addVertex(T.label, "A", "hubSite", true);
        Vertex a21 = this.sqlgGraph.addVertex(T.label, "A", "hubSite", false);
        Vertex a22 = this.sqlgGraph.addVertex(T.label, "A", "hubSite", false);
        Vertex a23 = this.sqlgGraph.addVertex(T.label, "A", "hubSite", false);

        a10.addEdge("a", a20);
        a11.addEdge("a", a21);
        a12.addEdge("a", a22);
        a13.addEdge("a", a23);

        Vertex a30 = this.sqlgGraph.addVertex(T.label, "A", "hubSite", true);
        Vertex a31 = this.sqlgGraph.addVertex(T.label, "A", "hubSite", false);

        a21.addEdge("a", a30);
        a22.addEdge("a", a31);

        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal()
                .V(a0)
                .repeat(__.out())
//                .until(__.or(__.loops().is(P.gt(3)), __.has("hubSite", true)))
                .until(__.has("hubSite", true))
                .toList();
        Assert.assertEquals(3, vertices.size());

        List<Path> paths = this.sqlgGraph.traversal()
                .V(a0)
                .repeat(__.out())
                .until(__.or(__.loops().is(P.gt(10)), __.has("hubSite", true)))
                .path()
                .toList();
        Assert.assertEquals(3, paths.size());
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 3 && p.get(0).equals(a0) && p.get(1).equals(a10) && p.get(2).equals(a20),
                p -> p.size() == 4 && p.get(0).equals(a0) && p.get(1).equals(a11) && p.get(2).equals(a21) && p.get(3).equals(a30),
                p -> p.size() == 2 && p.get(0).equals(a0) && p.get(1).equals(a14)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            Assert.assertTrue(path.isPresent());
            Assert.assertTrue(paths.remove(path.get()));
        }
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testUnoptimizedRepeatStep() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").until(__.has(T.label, "B")).repeat(__.out()).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(b1) && vertices.contains(b2));

        vertices = this.sqlgGraph.traversal().V().hasLabel("A").repeat(__.out()).until(__.has(T.label, "B")).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(b1) && vertices.contains(b2));
    }

    @Test
    public void testUnoptimizedRepeatStepUntilHasProperty() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1", "hub", true);
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2", "hub", true);
        a1.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").repeat(__.out()).until(__.has("hub", true)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(b1) && vertices.contains(b2));
    }

    @Test
    public void testUnoptimizedRepeatDeep() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "hub", true);
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "hub", false);
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "hub", false);
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "hub", false);
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c11 = this.sqlgGraph.addVertex(T.label, "C", "hub", false);
        Vertex c12 = this.sqlgGraph.addVertex(T.label, "C", "hub", false);
        Vertex c13 = this.sqlgGraph.addVertex(T.label, "C", "hub", false);
        b1.addEdge("bc", c11);
        b1.addEdge("bc", c12);
        b1.addEdge("bc", c13);
        Vertex c21 = this.sqlgGraph.addVertex(T.label, "C", "hub", false);
        Vertex c22 = this.sqlgGraph.addVertex(T.label, "C", "hub", false);
        Vertex c23 = this.sqlgGraph.addVertex(T.label, "C", "hub", false);
        b2.addEdge("bc", c21);
        b2.addEdge("bc", c22);
        b2.addEdge("bc", c23);
        Vertex c31 = this.sqlgGraph.addVertex(T.label, "C", "hub", false);
        Vertex c32 = this.sqlgGraph.addVertex(T.label, "C", "hub", false);
        Vertex c33 = this.sqlgGraph.addVertex(T.label, "C", "hub", false);
        b3.addEdge("bc", c31);
        b3.addEdge("bc", c32);
        b3.addEdge("bc", c33);
        Vertex d = this.sqlgGraph.addVertex(T.label, "D", "hub", true);
        c11.addEdge("cd", d);
        c12.addEdge("cd", d);
        c13.addEdge("cd", d);
        c21.addEdge("cd", d);
        c22.addEdge("cd", d);
        c23.addEdge("cd", d);
        c31.addEdge("cd", d);
        c32.addEdge("cd", d);
        c33.addEdge("cd", d);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .repeat(
                        __.out()
                )
                .until(__.has("hub", true))
                .toList();

        Assert.assertEquals(9, vertices.size());

    }

    @Test
    public void testUnoptimizedRepeatStepOnGraphyGremlin() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "hub", true);
        Vertex a21 = this.sqlgGraph.addVertex(T.label, "A", "hub", false);
        Vertex a22 = this.sqlgGraph.addVertex(T.label, "A", "hub", false);
        Vertex a23 = this.sqlgGraph.addVertex(T.label, "A", "hub", false);
        Vertex a31 = this.sqlgGraph.addVertex(T.label, "A", "hub", false);
        Vertex a32 = this.sqlgGraph.addVertex(T.label, "A", "hub", false);
        Vertex a33 = this.sqlgGraph.addVertex(T.label, "A", "hub", false);
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "hub", false);
        Vertex a5 = this.sqlgGraph.addVertex(T.label, "A", "hub", true);
        a1.addEdge("aa", a21);
        a1.addEdge("aa", a22);
        a1.addEdge("aa", a23);

        a21.addEdge("aa", a31);
        a21.addEdge("aa", a32);
        a21.addEdge("aa", a33);

        a31.addEdge("aa", a4);
        a4.addEdge("aa", a5);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .repeat(
                        __.out()
                ).until(
                        __.has("hub", true)
                )
                .toList();
        Assert.assertEquals(4, vertices.size());
        Assert.assertEquals(a5, vertices.get(0));
        Assert.assertEquals(a5, vertices.get(1));
        Assert.assertEquals(a5, vertices.get(2));
        Assert.assertEquals(a5, vertices.get(3));
    }
}
