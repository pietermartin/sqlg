package org.umlg.sqlg.jmh;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

public class SqlgBenchmark {

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }

    @Benchmark
    @Fork(value = 2)
    @Warmup(iterations = 2, time = 5)
    @Measurement(iterations = 5, time = 5)
    @BenchmarkMode(Mode.AverageTime)
    public void init(BenchMarkState benchMarkState, Blackhole blackhole) {
        // Do nothing
        for (int i = 0; i < 100_000; i++) {
            blackhole.consume(
                    benchMarkState.sqlgGraph.traversal().V()
                            .hasLabel("Person_0")
                            .out("pet_0")
                            .toList()
                            .size()
            );
        }
    }

}
