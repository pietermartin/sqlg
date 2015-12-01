package org.sqlg.benchmark;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assert;
import org.openjdk.jmh.annotations.*;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by pieter on 2015/11/24.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 0)
@Fork(value = 1)
@Measurement(iterations = 3, time = 5)
public class ServerSideBulkEdgeCreation extends BaseBenchmark {

    protected SqlgGraph sqlgGraph;
    protected GraphTraversalSource gt;

    @Benchmark
    public long bulkAddEdges() {
        int numberOfElement = 1_000_000;
        if (this.sqlgGraph.getSqlDialect().supportsBatchMode()) {
            this.sqlgGraph.tx().streamingBatchModeOn();
        }
        List<String> leftUids = new ArrayList<>();
        for (int i = 0; i < numberOfElement; i++) {
            String uid = UUID.randomUUID().toString();
            leftUids.add(uid);
            this.sqlgGraph.streamVertex(T.label, "A", "uid", uid);
        }
        this.sqlgGraph.tx().flush();
        List<String> rightUids = new ArrayList<>();
        for (int i = 0; i < numberOfElement; i++) {
            String uid = UUID.randomUUID().toString();
            rightUids.add(uid);
            this.sqlgGraph.streamVertex(T.label, "B", "uid", uid);
        }
        this.sqlgGraph.tx().commit();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph.tx().streamingBatchModeOn();
        SchemaTable a = SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "A");
        SchemaTable b = SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "B");
        List<Pair<String,String>> leftRight = new ArrayList<>();
        int count = 0;
        for (String leftUid : leftUids) {
            leftRight.add(Pair.of(leftUid, rightUids.get(count++)));
        }
        this.sqlgGraph.bulkAddEdges("A", "B", "AB", Pair.of("uid", "uid"), leftRight);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        Assert.assertEquals(numberOfElement, this.gt.V().hasLabel("A").has("uid", P.within(leftUids)).toList().size());
        return numberOfElement;
    }

    @Setup(Level.Iteration)
    public void setup() throws Exception {
        dropDb();
        this.sqlgGraph = getSqlgGraph();
        this.gt = this.sqlgGraph.traversal();

    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        closeSqlgGraph(this.sqlgGraph);
    }
}
