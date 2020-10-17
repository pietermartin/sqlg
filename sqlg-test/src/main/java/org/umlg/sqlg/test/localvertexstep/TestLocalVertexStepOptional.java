package org.umlg.sqlg.test.localvertexstep;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.step.SqlgVertexStep;
import org.umlg.sqlg.step.barrier.SqlgLocalStepBarrier;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Date: 2016/05/04
 * Time: 8:01 PM
 */
@SuppressWarnings({"rawtypes", "DuplicatedCode"})
public class TestLocalVertexStepOptional extends BaseTest {

    @Test
    public void testLocalVertexStepOptimized() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a11 = this.sqlgGraph.addVertex(T.label, "A", "name", "a11");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.gt
                .V(a11)
                .local(
                        __.optional(
                                __.out()
                        ).values("name")
                ).path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgLocalStepBarrier);
        SqlgLocalStepBarrier<?, ?> localStep = (SqlgLocalStepBarrier) traversal.getSteps().get(1);
        List<SqlgVertexStep> sqlgVertexStepCompileds = TraversalHelper.getStepsOfAssignableClassRecursively(SqlgVertexStep.class, localStep.getLocalChildren().get(0));
        Assert.assertEquals(1, sqlgVertexStepCompileds.size());
        SqlgVertexStep sqlgVertexStepCompiled = sqlgVertexStepCompileds.get(0);
        assertStep(sqlgVertexStepCompiled, false, false, true, true);

        Assert.assertEquals(1, paths.size());
        List<Predicate<Path>> pathsToAssert = Collections.singletonList(
                p -> p.size() == 2 && p.get(0).equals(a11) && p.get(1).equals("a11")
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            Assert.assertTrue(path.isPresent());
            Assert.assertTrue(paths.remove(path.get()));
        }
        Assert.assertTrue(paths.isEmpty());

        paths = this.gt.V(a1).local(__.out().out().values("name")).path().toList();
        Assert.assertEquals(1, paths.size());
        pathsToAssert = Collections.singletonList(
                p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals("c1")
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            Assert.assertTrue(path.isPresent());
            Assert.assertTrue(paths.remove(path.get()));
        }
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testLocalVertexStepNotOptimized() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.gt
                .V()
                .local(
                        __.optional(
                                __.where(__.has(T.label, "A")).out()
                        )
                ).path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgLocalStepBarrier);
        SqlgLocalStepBarrier<?, ?> localStep = (SqlgLocalStepBarrier) traversal.getSteps().get(1);
        List<SqlgVertexStep> sqlgVertexStepCompileds = TraversalHelper.getStepsOfAssignableClassRecursively(SqlgVertexStep.class, localStep.getLocalChildren().get(0));
        Assert.assertEquals(1, sqlgVertexStepCompileds.size());
        SqlgVertexStep sqlgVertexStepCompiled = sqlgVertexStepCompileds.get(0);
        assertStep(sqlgVertexStepCompiled, false, false, false, true);
        Assert.assertEquals(3, paths.size());
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1),
                p -> p.size() == 1 && p.get(0).equals(a2),
                p -> p.size() == 1 && p.get(0).equals(b1)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            Assert.assertTrue(path.isPresent());
            Assert.assertTrue(paths.remove(path.get()));
        }
        Assert.assertTrue(paths.isEmpty());
//        while (traversal.hasNext()) {
//            System.out.println(traversal.next());
//        }
    }

    @Test
    public void testLocalOptional2() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        a2.addEdge("ab", b2);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V()
                .local(
                        __.optional(
                                __.out()
                        ).optional(
                                __.out()
                        )
                )
                .path();
        List<Path> paths = traversal.toList();
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgLocalStepBarrier);
        SqlgLocalStepBarrier<?, ?> localStep = (SqlgLocalStepBarrier) traversal.getSteps().get(1);
        List<SqlgVertexStep> sqlgVertexSteps = TraversalHelper.getStepsOfAssignableClassRecursively(SqlgVertexStep.class, localStep.getLocalChildren().get(0));
        Assert.assertEquals(2, sqlgVertexSteps.size());
        SqlgVertexStep sqlgVertexStep = sqlgVertexSteps.get(0);
        //isForMultipleQueries is arbitary here.
        //Some will be true and some false, depending on the out from which vertex.
        assertStep(sqlgVertexStep, false, false, true);
        sqlgVertexStep = sqlgVertexSteps.get(1);
        //isForMultipleQueries is arbitary here.
        //Some will be true and some false, depending on the out from which vertex.
        assertStep(sqlgVertexStep, false, false, true);

        Assert.assertEquals(6, paths.size());
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1),
                p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1),
                p -> p.size() == 1 && p.get(0).equals(c1),
                p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(b2),
                p -> p.size() == 1 && p.get(0).equals(b2),
                p -> p.size() == 1 && p.get(0).equals(a3)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            Assert.assertTrue(path.isPresent());
            Assert.assertTrue(paths.remove(path.get()));
        }
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testLocalOptionalNested() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        a2.addEdge("ab", b2);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V()
                .local(
                        __.optional(
                                __.out().optional(
                                        __.out()
                                )
                        )
                )
                .path();
        List<Path> paths = traversal.toList();
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgLocalStepBarrier);
        SqlgLocalStepBarrier<?, ?> localStep = (SqlgLocalStepBarrier) traversal.getSteps().get(1);
        List<SqlgVertexStep> sqlgVertexStepCompileds = TraversalHelper.getStepsOfAssignableClassRecursively(SqlgVertexStep.class, localStep.getLocalChildren().get(0));
        Assert.assertEquals(1, sqlgVertexStepCompileds.size());
        SqlgVertexStep sqlgVertexStepCompiled = sqlgVertexStepCompileds.get(0);
        assertStep(sqlgVertexStepCompiled, false, false, true);
        Assert.assertEquals(6, paths.size());
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1),
                p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1),
                p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(b2),
                p -> p.size() == 1 && p.get(0).equals(c1),
                p -> p.size() == 1 && p.get(0).equals(b2),
                p -> p.size() == 1 && p.get(0).equals(a3)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            Assert.assertTrue(path.isPresent());
            Assert.assertTrue(paths.remove(path.get()));
        }
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testUnoptimizableChooseStep() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>)this.sqlgGraph.traversal()
                .V()
                .local(
                        __.<Vertex, Vertex>choose(
                                v -> v.label().equals("A"), __.out(), __.in()
                        )
                );
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgLocalStepBarrier);
        SqlgLocalStepBarrier<?, ?> localStep = (SqlgLocalStepBarrier) traversal.getSteps().get(1);
        List<SqlgVertexStep> sqlgVertexStepCompileds = TraversalHelper.getStepsOfAssignableClassRecursively(SqlgVertexStep.class, localStep.getLocalChildren().get(0));
        Assert.assertEquals(2, sqlgVertexStepCompileds.size());
        SqlgVertexStep sqlgVertexStepCompiled = sqlgVertexStepCompileds.get(0);
        assertStep(sqlgVertexStepCompiled, false, false, false, true);
        sqlgVertexStepCompiled = sqlgVertexStepCompileds.get(1);
        assertStep(sqlgVertexStepCompiled, false, false, false, true);
        Assert.assertEquals(4, vertices.size());
        Assert.assertEquals(2, vertices.stream().filter(v -> v.value("name").equals("a1")).count());
        Assert.assertEquals(1, vertices.stream().filter(v -> v.value("name").equals("b1")).count());
        Assert.assertEquals(1, vertices.stream().filter(v -> v.value("name").equals("b2")).count());
    }

    @Test
    public void testOptional() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(a1)
                .local(
                        __.optional(
                                __.out()
                        ).path()
                );
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgLocalStepBarrier);
        SqlgLocalStepBarrier<?, ?> localStep = (SqlgLocalStepBarrier) traversal.getSteps().get(1);
        List<SqlgVertexStep> sqlgVertexStepCompileds = TraversalHelper.getStepsOfAssignableClassRecursively(SqlgVertexStep.class, localStep.getLocalChildren().get(0));
        Assert.assertEquals(1, sqlgVertexStepCompileds.size());
        SqlgVertexStep sqlgVertexStepCompiled = sqlgVertexStepCompileds.get(0);
        //isForMultipleQueries is arbitary
        assertStep(sqlgVertexStepCompiled, false, false, true, true);
        Assert.assertEquals(2, paths.size());
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            Assert.assertTrue(path.isPresent());
            Assert.assertTrue(paths.remove(path.get()));
        }
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testOptionalNested() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();
        List<Path> paths = this.sqlgGraph.traversal()
                .V(a1)
                .local(
                        __.optional(
                                __.out().optional(
                                        __.out()
                                )
                        ).path()
                )
                .toList();
        Assert.assertEquals(2, paths.size());
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            Assert.assertTrue(path.isPresent());
            Assert.assertTrue(paths.remove(path.get()));
        }
        Assert.assertTrue(paths.isEmpty());

        paths = this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .local(
                        __.optional(
                                __.out().optional(
                                        __.out()
                                )
                        ).path()
                ).toList();
        Assert.assertEquals(3, paths.size());
        pathsToAssert = Arrays.asList(
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2),
                p -> p.size() == 1 && p.get(0).equals(a2)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            Assert.assertTrue(path.isPresent());
            Assert.assertTrue(paths.remove(path.get()));
        }
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testOptionalMultipleEdgeLabels() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);

        Vertex bb1 = this.sqlgGraph.addVertex(T.label, "BB");
        Vertex bb2 = this.sqlgGraph.addVertex(T.label, "BB");
        a1.addEdge("abb", bb1);
        a1.addEdge("abb", bb2);

        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal().V(a1).local(__.optional(__.out("ab", "abb")).path()).toList();
        Assert.assertEquals(4, paths.size());
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(bb1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(bb2)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            Assert.assertTrue(path.isPresent());
            Assert.assertTrue(paths.remove(path.get()));
        }
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testOptionalNestedMultipleEdgeLabels() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        b1.addEdge("bc", c1);

        Vertex bb1 = this.sqlgGraph.addVertex(T.label, "BB");
        Vertex bb2 = this.sqlgGraph.addVertex(T.label, "BB");
        Vertex cc1 = this.sqlgGraph.addVertex(T.label, "CC");
        a1.addEdge("abb", bb1);
        a1.addEdge("abb", bb2);
        bb1.addEdge("bbcc", cc1);

        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal().V(a1).local(__.optional(__.out("ab", "abb").optional(__.out("bc", "bbcc"))).path()).toList();
        Assert.assertEquals(4, paths.size());
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2),
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(bb1) && p.get(2).equals(cc1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(bb2)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            Assert.assertTrue(path.isPresent());
            Assert.assertTrue(paths.remove(path.get()));
        }
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testOptionalOnNonExistingEdgeLabel() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal().V(a1).local(__.optional(__.out("ab", "bb")).path()).toList();
        Assert.assertEquals(1, paths.size());
        List<Predicate<Path>> pathsToAssert = Collections.singletonList(
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            Assert.assertTrue(path.isPresent());
            Assert.assertTrue(paths.remove(path.get()));
        }
        Assert.assertTrue(paths.isEmpty());

        paths = this.sqlgGraph.traversal().V(a1).local(__.optional(__.out("bb")).path()).toList();
        Assert.assertEquals(1, paths.size());
        pathsToAssert = Collections.singletonList(
                p -> p.size() == 1 && p.get(0).equals(a1)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            Assert.assertTrue(path.isPresent());
            Assert.assertTrue(paths.remove(path.get()));
        }
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testMultipleNestedOptional() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex cc1 = this.sqlgGraph.addVertex(T.label, "CC");
        Vertex cc2 = this.sqlgGraph.addVertex(T.label, "CC");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        b2.addEdge("bcc", cc2);
        c1.addEdge("ccc", cc1);
        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal().V(a1)
                .local(
                        __.optional(
                                __.out("ab").optional(
                                        __.out("bc")
                                )
                        ).path()
                ).toList();
        Assert.assertEquals(2, paths.size());
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            Assert.assertTrue(path.isPresent());
            Assert.assertTrue(paths.remove(path.get()));
        }
        Assert.assertTrue(paths.isEmpty());

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>)this.sqlgGraph.traversal().V(a1)
                .local(
                        __.optional(
                                __.out("ab").optional(
                                        __.out("bc")
                                )
                        ).out().path()
                );
        paths = traversal.toList();
        Assert.assertEquals(2, paths.size());
        pathsToAssert = Arrays.asList(
                p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(cc1),
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b2) && p.get(2).equals(cc2)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            Assert.assertTrue(path.isPresent());
            Assert.assertTrue(paths.remove(path.get()));
        }
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testOptionalOutNotThere() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        b1.addEdge("knows", a1);
        this.sqlgGraph.tx().commit();
        List<Path> paths = this.sqlgGraph.traversal().V(a1).local(__.optional(__.out("knows")).path()).toList();
        Assert.assertEquals(1, paths.size());
    }

    @Test
    public void g_VX2X_optionalXoutXknowsXX() {
        loadModern(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
        Graph g = this.sqlgGraph;
        assertModernGraph(g, true, false);

        Object vadas = convertToVertexId(g, "vadas");
        Vertex vadasVertex = g.traversal().V(vadas).next();
        List<Path> paths = g.traversal().V(vadasVertex).local(__.optional(__.out("knows")).path()).toList();
        Assert.assertEquals(1, paths.size());

        List<Predicate<Path>> pathsToAssert = Collections.singletonList(
                p -> p.size() == 1 && p.get(0).equals(vadasVertex)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            Assert.assertTrue(path.isPresent());
            Assert.assertTrue(paths.remove(path.get()));
        }
        Assert.assertTrue(paths.isEmpty());

        List<Vertex> vertices = g.traversal().V(vadasVertex).local(__.optional(__.out("knows"))).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(vadasVertex, vertices.get(0));

        paths = g.traversal().V().local(__.optional(__.out().optional(__.out())).path()).toList();
        for (Path path : paths) {
            System.out.println(path.toString());
        }
        Assert.assertEquals(10, paths.size());
        pathsToAssert = Arrays.asList(
                p -> p.size() == 2 && p.get(0).equals(convertToVertex(g, "marko")) && p.get(1).equals(convertToVertex(g, "lop")),
                p -> p.size() == 2 && p.get(0).equals(convertToVertex(g, "marko")) && p.get(1).equals(convertToVertex(g, "vadas")),
                p -> p.size() == 3 && p.get(0).equals(convertToVertex(g, "marko")) && p.get(1).equals(convertToVertex(g, "josh")) && p.get(2).equals(convertToVertex(g, "lop")),
                p -> p.size() == 3 && p.get(0).equals(convertToVertex(g, "marko")) && p.get(1).equals(convertToVertex(g, "josh")) && p.get(2).equals(convertToVertex(g, "ripple")),
                p -> p.size() == 1 && p.get(0).equals(convertToVertex(g, "vadas")),
                p -> p.size() == 1 && p.get(0).equals(convertToVertex(g, "lop")),
                p -> p.size() == 2 && p.get(0).equals(convertToVertex(g, "josh")) && p.get(1).equals(convertToVertex(g, "lop")),
                p -> p.size() == 2 && p.get(0).equals(convertToVertex(g, "josh")) && p.get(1).equals(convertToVertex(g, "ripple")),
                p -> p.size() == 1 && p.get(0).equals(convertToVertex(g, "ripple")),
                p -> p.size() == 2 && p.get(0).equals(convertToVertex(g, "peter")) && p.get(1).equals(convertToVertex(g, "lop"))
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            Assert.assertTrue(path.isPresent());
            Assert.assertTrue(paths.remove(path.get()));
        }
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testCurrentTreeLabelToSelf1() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("aa", a2);
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal().V().local(__.optional(__.out().optional(__.out())).path()).toList();
        for (Path path : paths) {
            System.out.println(path.toString());
        }
        Assert.assertEquals(4, paths.size());

        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2),
                p -> p.size() == 1 && p.get(0).equals(a2),
                p -> p.size() == 1 && p.get(0).equals(b1)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            Assert.assertTrue(path.isPresent());
            Assert.assertTrue(paths.remove(path.get()));
        }
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testCurrentTreeLabelToSelfSimple() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("aa", a1);
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal().V(a1).local(__.optional(__.out().optional(__.out())).path()).toList();
        for (Path path : paths) {
            System.out.println(path.toString());
        }
        Assert.assertEquals(3, paths.size());

        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a1) && p.get(2).equals(a1),
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a1) && p.get(2).equals(b1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            Assert.assertTrue(path.isPresent());
            Assert.assertTrue(paths.remove(path.get()));
        }
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testCurrentTreeLabelToSelf() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("aa", a1);
        a1.addEdge("aa", a1);
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b1);

        List<Path> paths = this.sqlgGraph.traversal().V(a1).local(__.optional(__.out().optional(__.out())).path()).toList();
        for (Path path : paths) {
            System.out.println(path.toString());
        }
        Assert.assertEquals(10, paths.size());

        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a1) && p.get(2).equals(a1),
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a1) && p.get(2).equals(a1),
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a1) && p.get(2).equals(a1),
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a1) && p.get(2).equals(a1),
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a1) && p.get(2).equals(b1),
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a1) && p.get(2).equals(b1),
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a1) && p.get(2).equals(b1),
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a1) && p.get(2).equals(b1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            Assert.assertTrue(path.isPresent());
            Assert.assertTrue(paths.remove(path.get()));
        }
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testOptionalLeftJoin() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        b1.addEdge("bc", c1);
        b2.addEdge("bd", d1);
        this.sqlgGraph.tx().commit();
        List<Path> paths = this.sqlgGraph.traversal()
                .V(a1)
                .local(
                        __.optional(
                                __.out().optional(
                                        __.out()
                                )
                        ).path()
                )
                .toList();
        for (Path path : paths) {
            System.out.println(path.toString());
        }
        Assert.assertEquals(3, paths.size());

        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1),
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b2) && p.get(2).equals(d1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            Assert.assertTrue(path.isPresent());
            Assert.assertTrue(paths.remove(path.get()));
        }
        Assert.assertTrue(paths.isEmpty());
    }
}
