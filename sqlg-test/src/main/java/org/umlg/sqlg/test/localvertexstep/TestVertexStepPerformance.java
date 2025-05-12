package org.umlg.sqlg.test.localvertexstep;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.umlg.sqlg.structure.DefaultSqlgTraversal;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;
import java.util.Map;

/**
 * Date: 2017/04/17
 * Time: 11:30 AM
 */
class TestVertexStepPerformance extends BaseTest {

//    @Test
    public void testLocalVertexStepNotOptimizedPerformance() {
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 10_000; i++) {
            Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
            Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
            Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
            a1.addEdge("ab", b1);
        }
        this.sqlgGraph.tx().commit();

        StopWatch stopWatch = new StopWatch();
        for (int i = 0; i < 1000; i++) {
            DefaultSqlgTraversal<Vertex, Path> traversal = (DefaultSqlgTraversal<Vertex, Path>) this.gt
                    .V()
                    .local(
                            __.optional(
                                    __.where(__.has(T.label, "A")).out()
                            )
                    ).path();
            stopWatch.start();
            List<Path> paths = traversal.toList();
            stopWatch.stop();
            System.out.println(stopWatch.toString());
            stopWatch.reset();
            Assert.assertEquals(30_000, paths.size());
        }
    }

//    @Test
    public void testBatchingIncomingTraversersOnVertexStep() {
        this.sqlgGraph.tx().normalBatchModeOn();
        int count = 10;
        for (int i = 0; i < count; i++) {
            Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
            for (int j = 0; j < 10_000; j++) {
                Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
                a1.addEdge("ab", b1);
            }
        }
        this.sqlgGraph.tx().commit();

        StopWatch stopWatch = new StopWatch();
        for (int i = 0; i < 10_000; i++) {
            stopWatch.start();
            GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal()
                    .V()
                    .where(__.hasLabel("A"))
                    .out();
            List<Vertex> vertices = traversal.toList();
            stopWatch.stop();
            System.out.println(stopWatch.toString());
            stopWatch.reset();
            Assert.assertEquals(10_000 * 10, vertices.size());

        }
    }

//    @Test
    public void testPreformanceBatchingIncomingTraversersOnLocalVertexStep() {
        this.sqlgGraph.tx().normalBatchModeOn();
        int count = 10_000;
        for (int i = 0; i < count; i++) {
            Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
            a1.addEdge("ab", b1);
        }
        this.sqlgGraph.tx().commit();

        for (int i = 0; i < 10000; i++) {
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal()
                    .V().hasLabel("A")
                    .local(
                            __.out()
                    );
            List<Vertex> vertices = traversal.toList();
            stopWatch.stop();
            System.out.println(stopWatch.toString());
            Assert.assertEquals(count, vertices.size());
        }
    }

    //TODO group is not optimized
//    @Test
    public void testGroup() {
        loadGratefulDead();

        for (int i = 0; i < 10000; i++) {
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            DefaultSqlgTraversal<Vertex, Map<String, Map<String, Number>>> traversal = (DefaultSqlgTraversal<Vertex, Map<String, Map<String, Number>>>) this.sqlgGraph.traversal()
                    .V()
                    .out("followedBy")
                    .<String, Map<String, Number>>group()
                    .by("songType")
                    .by(
                            __.bothE()
                                    .group()
                                    .by(T.label)
                                    .by(
                                            __.values("weight").sum()
                                    )
                    );

            final Map<String, Map<String, Number>> map = traversal.next();
            stopWatch.stop();
            System.out.println(stopWatch.toString());
        }
    }

}
