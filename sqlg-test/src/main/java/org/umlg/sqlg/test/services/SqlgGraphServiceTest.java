/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.umlg.sqlg.test.services;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.services.SqlgDegreeCentralityFactory;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


/**
 * Demonstration of Service API.
 *
 * @author Mike Personick (http://github.com/mikepersonick)
 */
public class SqlgGraphServiceTest extends BaseTest {

    GraphTraversalSource g;

    @Before
    public void registerServices() throws Exception {
        super.before();
        sqlgGraph.getServiceRegistry().registerService(new SqlgDegreeCentralityFactory(sqlgGraph));
        loadModern();
        g = sqlgGraph.traversal();
    }

//    /**
//     * List all available service calls.
//     */
//    @Test
//    public void g_call() throws Exception {
//        /*
//         * Tinkergraph has one registered service by default ("tinker.search").
//         */
//        assertArrayEquals(new String[] {
//                "sqlg.degree.centrality"
//                }, toResultStrings(
//
//                g.call()
//
//        ));
//        assertArrayEquals(new String[] {
//                "sqlg.degree.centrality"
//                }, toResultStrings(
//
//                g.call("--list")
//
//        ));
//
//    }

    @Test
    public void g_V_call_degree_centrality() {
        List<Object> result = g.V().as("v").call("sqlg.degree.centrality").toList();
        for (Object o : result) {
            System.out.println(o.toString());
        }
        assertArrayEquals(new String[] {
                "{vertex=v[1], degree=0}",
                "{vertex=v[2], degree=1}",
                "{vertex=v[3], degree=3}",
                "{vertex=v[4], degree=1}",
                "{vertex=v[5], degree=1}",
                "{vertex=v[6], degree=0}",
        }, toResultStrings(

                g.V().as("v").call("sqlg.degree.centrality")
                        .project("vertex", "degree").by(__.select("v")).by()

        ));

        assertArrayEquals(new String[] {
                "{vertex=marko, degree=3}",
                "{vertex=vadas, degree=0}",
                "{vertex=lop, degree=0}",
                "{vertex=josh, degree=2}",
                "{vertex=ripple, degree=0}",
                "{vertex=peter, degree=1}",
        }, toResultStrings(

                g.V().as("v").call("tinker.degree.centrality").with("direction", Direction.OUT)
                        .project("vertex", "degree").by(__.select("v").values("name")).by()

        ));

        checkResult("lop", g.V().where(__.call("tinker.degree.centrality").is(3)).values("name"));
        checkResults(Arrays.asList("vadas","josh","ripple"), g.V().where(__.call("tinker.degree.centrality").is(1)).values("name"));
        checkResult(0l, g.V().where(__.call("tinker.degree.centrality").is(100)).count());
    }

//    /**
//     * Demonstrates registration and usage of a Starting lambda service. Start services take no input and are run
//     * via GraphTraversalSource. Text search above is another example of a Start service.
//     */
//    @Test
//    public void g_call_start_lambda() {
//        final String serviceName = "tinker.random";
//
//        final BiFunction<ServiceCallContext, Map, Iterator<Integer>> lambda = (ctx, map) -> {
//            final int numValues = (int) map.getOrDefault("numValues", 5);
//            final boolean random = (boolean) map.getOrDefault("random", false);
//            final Long seed = (Long) map.get("seed");
//            final List<Integer> values = new ArrayList<>(numValues);
//            final Random r = seed != null ? new Random(seed) : new Random();
//            for (int i = 0; i < numValues; i++) {
//                if (random)
//                    values.add(r.nextInt(1000));
//                else
//                    // use fixed values for testing
//                    values.add(i);
//            }
//            return values.iterator();
//        };
//
//        sqlgGraph.getServiceRegistry().<Object,Integer>registerLambdaService(serviceName).addStartLambda(lambda);
//
//        /*
//         * Use defaults.
//         */
//        assertEquals("[0,1,2,3,4]", toResultString(
//
//            g.call(serviceName)
//
//        ));
//
//        /*
//         * Invoke via Map (static parameters).
//         */
//        assertEquals("[0,1,2,3,4,5,6,7,8,9]", toResultString(
//
//            g.call(serviceName, asMap("numValues", 10))
//
//        ));
//
//        /*
//         * Invoke via Traversal<Map> (dynamic parameters).
//         */
//        assertEquals("[0,1,2,3,4,5,6,7]", toResultString(
//
//            g.withSideEffect("x", asMap("numValues", 8))
//                .call(serviceName, __.select("x"))
//
//        ));
//
//        /*
//         * Invoke via .with(String, Traversal) (dynamic parameters).
//         */
//        checkResult(100l,
//
//                g.call(serviceName)
//                    .with("random", true)
//                    .with("numValues", 100)
//                    .with("seed", __.V().constant(17537423l))
//                        .count()
//
//        );
//    }
//
//    /**
//     * Demonstrates registration and usage of a Streaming lambda service. Streaming services produce an iterator
//     * of results for each upstream input.
//     */
//    @Test
//    public void g_V_call_streaming_lambda() {
//        final String serviceName = "tinker.xerox";
//
//        final TriFunction<ServiceCallContext, Traverser.Admin<Object>,Map,Iterator<Object>> lambda = (ctx, traverser, map) -> {
//            final int numCopies = (int) map.getOrDefault("numCopies", 2);
//            if (ctx.getTraversal().getTraverserRequirements().contains(TraverserRequirement.BULK)) {
//                traverser.setBulk(traverser.bulk() * numCopies);
//                return IteratorUtils.of(traverser);
//            } else {
//                return IntStream.range(0, numCopies).mapToObj(i -> traverser.get()).iterator();
//            }
//        };
//
//        sqlgGraph.getServiceRegistry().registerLambdaService(serviceName).addStreamingLambda(lambda);
//
//        checkResult(6l,
//            g.V().count());
//
//        checkResult(12l,
//            g.V().call(serviceName).count());
//
//        checkResult(18l,
//            g.V().call(serviceName).with("numCopies", 3).count());
//    }
//
//    /**
//     * Demonstrates registration and usage of a Barrier lambda service. Barrier services collect all upstream input
//     * and produce an iterator of results. Barrier services can reduce, expand or pass-through their input.
//     */
//    @Test
//    public void g_V_call_barrier_lambda() {
//        final String serviceName = "tinker.order";
//
//        final Comparator defaultComparator = Comparator.comparing(Object::toString);
//
//        final TriFunction<ServiceCallContext, TraverserSet<Object>, Map, Iterator<Traverser.Admin<Object>>> lambda =
//                (ctx, traverserSet, map) -> {
//            final Comparator comparator = (Comparator) map.getOrDefault("comparator", defaultComparator);
//            traverserSet.sort(comparator);
//            return traverserSet.iterator();
//        };
//
//        sqlgGraph.getServiceRegistry().<Object,Traverser.Admin<Object>>registerLambdaService(serviceName).addBarrierLambda(lambda);
//
//        assertArrayEquals(new String[] {
//                "vp[age->27]",
//                "vp[age->29]",
//                "vp[age->32]",
//                "vp[age->35]",
//                "vp[lang->java]",
//                "vp[lang->java]",
//                "vp[name->josh]",
//                "vp[name->lop]",
//                "vp[name->marko]",
//                "vp[name->peter]",
//                "vp[name->ripple]",
//                "vp[name->vadas]",
//                }, toResultStrings(
//
//            g.V().properties().call(serviceName)
//
//        ));
//
//        assertArrayEquals(new String[] {
//                "vp[name->vadas]",
//                "vp[name->ripple]",
//                "vp[name->peter]",
//                "vp[name->marko]",
//                "vp[name->lop]",
//                "vp[name->josh]",
//                "vp[lang->java]",
//                "vp[lang->java]",
//                "vp[age->35]",
//                "vp[age->32]",
//                "vp[age->29]",
//                "vp[age->27]",
//                }, toResultStrings(
//
//            g.V().properties().call(serviceName, asMap("comparator", defaultComparator.reversed()))
//
//        ));
//    }
//
//    /**
//     * Services may need to specify traverser requirements.
//     */
//    @Test
//    public void g_V_call_lambda_with_requirements() throws Exception {
//        final String serviceName = "tinker.paths";
//
//        final Map describeParams = asMap(
//                "shortest", "Boolean parameter, if true, calculate shortest path",
//                "longest", "Boolean parameter, if true, calculate longest path (default)");
//
//        final TriFunction<ServiceCallContext, TraverserSet<Vertex>, Map, Iterator<Traverser.Admin<Vertex>>> lambda =
//                (ctx, traverserSet, map) -> {
//            final boolean shortest = (boolean) map.getOrDefault("shortest", false);
//            final boolean longest = (boolean) map.getOrDefault("longest", !shortest);
//
//            int min = Integer.MAX_VALUE;
//            int max = 0;
//            for (Traverser.Admin t : traverserSet) {
//                final int length = t.path().size();
//                min = Math.min(min, length);
//                max = Math.max(max, length);
//            }
//            final Iterator<Traverser.Admin<Vertex>> it = traverserSet.iterator();
//            while (it.hasNext()) {
//                final int length = it.next().path().size();
//                if (shortest && length == min || longest && length == max) {
//                    // keep
//                } else {
//                    it.remove();
//                }
//            }
//            return traverserSet.iterator();
//        };
//
//        sqlgGraph.getServiceRegistry().registerLambdaService(serviceName)
//                .addDescribeParams(describeParams)
//                        .addBarrierLambda(lambda)
//                                .addRequirements(TraverserRequirement.PATH);
//
//        checkResult("{\"name\":\"tinker.paths\",\"type:[requirements]:\":{\"Barrier\":[\"PATH\"]},\"params\":"+new ObjectMapper().writeValueAsString(describeParams)+"}",
//            g.call("--list").with("service", serviceName).with("verbose"));
//
//        checkResult(2l, g.V().repeat(out()).until(outE().count().is(0)).call(serviceName).count());
//        checkResult(5l, g.V().repeat(out()).until(outE().count().is(0)).call(serviceName).with("shortest").count());
//        checkResult(7l, g.V().repeat(out()).until(outE().count().is(0)).call(serviceName).with("shortest").with("longest").count());
//
//        assertArrayEquals(new String[] {
//                "path[v[1], v[4], v[5]]",
//                "path[v[1], v[4], v[3]]"
//                }, toResultStrings(
//
//                g.V().repeat(out()).until(outE().count().is(0)).call(serviceName).path()
//
//        ));
//    }
//
//    @Test
//    public void testMultiTypeService() {
//        final String serviceName = "tinker.multi-type";
//
//        final TriFunction<ServiceCallContext, TraverserSet<Object>, Map, Iterator<Integer>> barrier =
//                (ctx, traverserSet, map) -> IteratorUtils.of(traverserSet.size());
//
//        final TriFunction<ServiceCallContext, Traverser.Admin<Object>,Map,Iterator<Object>> streaming =
//                (ctx, traverser, map) -> IteratorUtils.of(1);
//
//        final BiFunction<ServiceCallContext, Map, Iterator<Integer>> start =
//                (ctx, map) -> IteratorUtils.of(0);
//
//        final SqlgServiceRegistry.LambdaServiceFactory factory = sqlgGraph.getServiceRegistry().registerLambdaService(serviceName);
//        factory.addStartLambda(start);
//        factory.addStreamingLambda(streaming);
//        factory.addBarrierLambda(barrier);
//
//        checkResult("{\"name\":\"tinker.multi-type\",\"type:[requirements]:\":{\"Start\":[],\"Streaming\":[],\"Barrier\":[]},\"params\":{}}",
//                g.call("--list").with("service", serviceName).with("verbose"));
//
//        // start
//        checkResult(0, g.call(serviceName));
//        // streaming
//        checkResults(Arrays.asList(1, 1, 1, 1, 1, 1), g.V().call(serviceName, asMap(SqlgServiceRegistry.LambdaServiceFactory.Options.TYPE, Type.Streaming)));
//        // all at once
//        checkResults(Arrays.asList(6), g.V().call(serviceName, asMap(SqlgServiceRegistry.LambdaServiceFactory.Options.TYPE, Type.Barrier)));
//        // chunk size 2
//        checkResults(Arrays.asList(2, 2, 2), g.V().call(serviceName, asMap(SqlgServiceRegistry.LambdaServiceFactory.Options.TYPE, Type.Barrier, SqlgServiceRegistry.LambdaServiceFactory.Options.CHUNK_SIZE, 2)));
//        // chunk size 4
//        checkResults(Arrays.asList(4, 2), g.V().call(serviceName, asMap(SqlgServiceRegistry.LambdaServiceFactory.Options.TYPE, Type.Barrier, SqlgServiceRegistry.LambdaServiceFactory.Options.CHUNK_SIZE, 4)));
//    }
//
    private String toResultString(final Traversal traversal) {
        return (String) IteratorUtils.stream(traversal).map(Object::toString).collect(Collectors.joining(",", "[", "]"));
    }

    private String[] toResultStrings(final Traversal traversal) {
        return (String[]) ((List) IteratorUtils.stream(traversal).map(Object::toString).collect(Collectors.toList())).toArray(new String[0]);
    }

    private void checkResult(final Object expected, final Traversal traversal) {
        final List result = traversal.toList();
        assertEquals("Did not produce exactly one result", 1, result.size());
        assertEquals(expected, result.get(0));
    }

}
