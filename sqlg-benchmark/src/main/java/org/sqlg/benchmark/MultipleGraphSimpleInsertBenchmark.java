package org.sqlg.benchmark;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.openjdk.jmh.annotations.*;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgUtil;

import java.util.HashMap;
import java.util.Map;

@State(Scope.Group)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 0)
@Fork(value = 1)
@Measurement(iterations = 10, time = 5)
public class MultipleGraphSimpleInsertBenchmark extends BaseBenchmark {

    protected SqlgGraph sqlgGraph1;
    protected SqlgGraph sqlgGraph2;
    protected GraphTraversalSource gt2;

    @Group("multipleInsert")
    @GroupThreads(10)
    @Benchmark
    public long insert1000VerticesWith10Properties1() {
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
        for (int i = 0; i < 1000; i++) {
            this.sqlgGraph1.addVertex("Person", properties);
        }
        this.sqlgGraph1.tx().commit();
        return 1000;
    }

    @Group("multipleInsert")
    @GroupThreads(10)
    @Benchmark
    public long insert1000VerticesWith10Properties2() {
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
        for (int i = 0; i < 1000; i++) {
            this.sqlgGraph2.addVertex("Person", properties);
        }
        this.sqlgGraph2.tx().commit();
        return 1000;
    }

    @Setup(Level.Iteration)
    public void setup() throws Exception {
        System.out.println("setup");
        this.sqlgGraph1 = getSqlgGraph(true);
        SqlgUtil.dropDb(this.sqlgGraph1);
        this.sqlgGraph1.tx().commit();
        this.sqlgGraph1 = getSqlgGraph(true);
        this.sqlgGraph2 = getSqlgGraph(true);
        this.gt2 = this.sqlgGraph2.traversal();
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        System.out.println("tearDown");
        closeSqlgGraph(this.sqlgGraph1);
        closeSqlgGraph(this.sqlgGraph2);
    }
}
