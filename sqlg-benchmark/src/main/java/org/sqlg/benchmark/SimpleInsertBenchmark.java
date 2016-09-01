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

import org.openjdk.jmh.annotations.*;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgUtil;

import java.util.HashMap;
import java.util.Map;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 0)
@Fork(value = 1)
@Measurement(iterations = 10, time = 5)
public class SimpleInsertBenchmark extends BaseBenchmark {

    protected SqlgGraph sqlgGraph;

    @Benchmark
    public long insert1000VerticesWith10Properties() {
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
            this.sqlgGraph.addVertex("Person", properties);
        }
        this.sqlgGraph.tx().commit();
        return 1000;
    }

    @Setup(Level.Iteration)
    public void setup() throws Exception {
        this.sqlgGraph = getSqlgGraph();
        SqlgUtil.dropDb(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph = getSqlgGraph();
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        closeSqlgGraph(this.sqlgGraph);
    }

}
