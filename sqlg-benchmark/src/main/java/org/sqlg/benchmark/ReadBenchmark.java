package org.sqlg.benchmark;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.openjdk.jmh.annotations.*;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.SqlgVertex;
import org.umlg.sqlg.util.SqlgUtil;

import java.util.HashMap;
import java.util.Map;

@State(Scope.Group)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 0)
@Fork(value = 1)
@Measurement(iterations = 10, time = 5)
public class ReadBenchmark extends BaseBenchmark {

    private SqlgGraph sqlgGraph;
    private GraphTraversalSource gt;

    @Group("readGraphStep")
    @GroupThreads(10)
    @Benchmark
    public long readGraphStep() {
        try {
            Long count = this.gt.V().hasLabel("A").out().out().count().next();
            if (count != 10000)
                throw new RuntimeException("expected 10000");
            return count;
        } finally {
            this.sqlgGraph.tx().rollback();
        }
    }

    @Group("readVertexStep")
    @GroupThreads(10)
    @Benchmark
    public long readVertexStep() {
        try {
            Vertex a = this.gt.V().hasLabel("A").next();
            Long count = this.gt.V(a).out().out().count().next();
            if (count != 10000)
                throw new RuntimeException("expected 10000");
            return count;
        } finally {
            this.sqlgGraph.tx().rollback();
        }
    }

    @Setup(Level.Iteration)
    public void setup() {
        System.out.println("setup");
        this.sqlgGraph = getSqlgGraph();
        SqlgUtil.dropDb(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph = getSqlgGraph();
        this.gt = this.sqlgGraph.traversal();

        //Create a graph with degree 3
        Map<String, Object> properties = new HashMap<>();
        properties.put("a1", "aaaaaaaaaa1");
        properties.put("a2", "aaaaaaaaaa2");
        properties.put("a3", "aaaaaaaaaa3");
        properties.put("a4", "aaaaaaaaaa4");
        properties.put("a5", "aaaaaaaaaa5");
        properties.put("a6", "aaaaaaaaaa6");
        properties.put("a7", "aaaaaaaaaa7");
        properties.put("a8", "aaaaaaaaaa8");
        properties.put("a9", "aaaaaaaaaa9");
        properties.put("a10", "aaaaaaaaaa10");

        SqlgVertex a = (SqlgVertex) this.sqlgGraph.addVertex("A", properties);
        for (int i = 0; i < 100; i++) {
            Vertex b = this.sqlgGraph.addVertex("B", properties);
            a.addEdge("ab", b);
            for (int j = 0; j < 100; j++) {
                Vertex c = this.sqlgGraph.addVertex("C", properties);
                b.addEdge("bc", c);
            }
        }
        this.sqlgGraph.tx().commit();
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        System.out.println("tearDown");
        closeSqlgGraph(this.sqlgGraph);
    }
}
