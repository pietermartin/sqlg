/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sqlg.benchmark;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.openjdk.jmh.annotations.*;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 0)
@Fork(value = 1)
@Measurement(iterations = 10, time = 5)
public class WithinBenchmark extends BaseBenchmark {

    protected SqlgGraph sqlgGraph;
    protected GraphTraversalSource gt;
    private List<String> uids = new ArrayList<>();
    private List<String> smallUidSet = new ArrayList<>();
    private int count = 1;

    @Benchmark
    public long withinWithIn() {
        this.sqlgGraph.configuration().setProperty("bulk.within.count", this.count);
        List<Vertex> vertices = this.gt.V().hasLabel("Person").has("uid", P.within(this.smallUidSet)).toList();
        Assert.assertEquals(this.count, vertices.size());
        return 1000000;
    }

    @Benchmark
    public long withinWithJoin() {
        this.sqlgGraph.configuration().setProperty("bulk.within.count", this.count - 1);
        List<Vertex> vertices = this.gt.V().hasLabel("Person").has("uid", P.within(this.smallUidSet)).toList();
        Assert.assertEquals(this.count, vertices.size());
        return 1000000;
    }

    @Setup(Level.Iteration)
    public void setup() throws Exception {
        this.uids.clear();
        this.smallUidSet.clear();
        this.sqlgGraph = getSqlgGraph();
        this.sqlgGraph.drop();
        this.sqlgGraph.tx().commit();
        this.sqlgGraph = getSqlgGraph();
        this.gt = this.sqlgGraph.traversal();
        if (this.sqlgGraph.getSqlDialect().supportsBatchMode()) {
            this.sqlgGraph.tx().normalBatchModeOn();
        }
        for (int i = 0; i < 1_000_000; i++) {
            String uid = UUID.randomUUID().toString();
            this.uids.add(uid);
            this.sqlgGraph.addVertex(T.label, "Person", "uid", uid);
        }
        this.sqlgGraph.tx().commit();
        for (int i = 0; i < this.count; i++) {
            this.smallUidSet.add(this.uids.get(i));
        }
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        closeSqlgGraph(this.sqlgGraph);
    }

}
