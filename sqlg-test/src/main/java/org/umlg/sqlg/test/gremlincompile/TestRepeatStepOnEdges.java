package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.tinkerpop.gremlin.process.traversal.Pop.last;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.local;

/**
 * Date: 2016/10/25
 * Time: 5:04 PM
 */
public class TestRepeatStepOnEdges extends BaseTest {

    private final Logger logger = LoggerFactory.getLogger(TestRepeatStepOnEdges.class);

    @Test
    public void test() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Edge, Edge> traversal = (DefaultGraphTraversal<Edge, Edge>) this.sqlgGraph.traversal().E().repeat(__.outV().outE()).times(2).emit();
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Edge> edges = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(2, edges.size());
    }

    @Test
    public void testBug116() {
        Vertex v0 = this.sqlgGraph.addVertex("code", "0");
        Vertex v1 = this.sqlgGraph.addVertex("code", "1");
        Vertex v2 = this.sqlgGraph.addVertex("code", "2");
        Vertex v3 = this.sqlgGraph.addVertex("code", "3");
        v0.addEdge("tsw", v1, "speed", "1", "arrTime", 10L, "depTime", 5L);
        v1.addEdge("tsw", v2, "speed", "1", "arrTime", 15L, "depTime", 9L); //must be ignored in longest path
        v1.addEdge("tsw", v2, "speed", "1", "arrTime", 20L, "depTime", 17L); //must be used in longest path
        v2.addEdge("tsw", v3, "speed", "1", "arrTime", 30L, "depTime", 25L);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal gp = (DefaultGraphTraversal) this.sqlgGraph.traversal().V().outE("tsw").as("e").inV().emit().repeat(
                __.outE("tsw").as("e").inV().simplePath()
        ).times(20);
        Assert.assertEquals(4, gp.getSteps().size());

        //noinspection unchecked
        Assert.assertEquals(10, IteratorUtils.list(gp).size());
        Assert.assertEquals(2, gp.getSteps().size());

        gp = query1(this.sqlgGraph.traversal());
        checkResult(gp);

        //check paths
        gp = query1(this.sqlgGraph.traversal());
        //check paths
        @SuppressWarnings("unchecked") List<List<Object>> paths = IteratorUtils.list(gp);
        List<Predicate<List<Object>>> pathsToAssert = Arrays.asList(
                p -> p.size() == 2 && p.get(0).equals("1") && ((Path) p.get(1)).get(0).equals(v0.id())
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("depTime") && ((Map) ((Path) p.get(1)).get(1)).get("depTime").equals(5L)
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("arrTime") && ((Map) ((Path) p.get(1)).get(1)).get("arrTime").equals(10L)
                        && ((Path) p.get(1)).get(2).equals(v1.id()),

                p -> p.size() == 2 && p.get(0).equals("1") && ((Path) p.get(1)).get(0).equals(v0.id())
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("depTime") && ((Map) ((Path) p.get(1)).get(1)).get("depTime").equals(5L)
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("arrTime") && ((Map) ((Path) p.get(1)).get(1)).get("arrTime").equals(10L)
                        && ((Path) p.get(1)).get(2).equals(v1.id())
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("depTime") && ((Map) ((Path) p.get(1)).get(3)).get("depTime").equals(17L)
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("arrTime") && ((Map) ((Path) p.get(1)).get(3)).get("arrTime").equals(20L)
                        && ((Path) p.get(1)).get(4).equals(v2.id()),

                p -> p.size() == 2 && p.get(0).equals("1") && ((Path) p.get(1)).get(0).equals(v0.id())
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("depTime") && ((Map) ((Path) p.get(1)).get(1)).get("depTime").equals(5L)
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("arrTime") && ((Map) ((Path) p.get(1)).get(1)).get("arrTime").equals(10L)
                        && ((Path) p.get(1)).get(2).equals(v1.id())
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("depTime") && ((Map) ((Path) p.get(1)).get(3)).get("depTime").equals(17L)
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("arrTime") && ((Map) ((Path) p.get(1)).get(3)).get("arrTime").equals(20L)
                        && ((Path) p.get(1)).get(4).equals(v2.id())
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("depTime") && ((Map) ((Path) p.get(1)).get(5)).get("depTime").equals(25L)
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("arrTime") && ((Map) ((Path) p.get(1)).get(5)).get("arrTime").equals(30L)
                        && ((Path) p.get(1)).get(6).equals(v3.id()),

                p -> p.size() == 2 && p.get(0).equals("1") && ((Path) p.get(1)).get(0).equals(v1.id())
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("depTime") && ((Map) ((Path) p.get(1)).get(1)).get("depTime").equals(9L)
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("arrTime") && ((Map) ((Path) p.get(1)).get(1)).get("arrTime").equals(15L)
                        && ((Path) p.get(1)).get(2).equals(v2.id()),

                p -> p.size() == 2 && p.get(0).equals("1") && ((Path) p.get(1)).get(0).equals(v1.id())
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("depTime") && ((Map) ((Path) p.get(1)).get(1)).get("depTime").equals(9L)
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("arrTime") && ((Map) ((Path) p.get(1)).get(1)).get("arrTime").equals(15L)
                        && ((Path) p.get(1)).get(2).equals(v2.id())
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("depTime") && ((Map) ((Path) p.get(1)).get(3)).get("depTime").equals(25L)
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("arrTime") && ((Map) ((Path) p.get(1)).get(3)).get("arrTime").equals(30L)
                        && ((Path) p.get(1)).get(4).equals(v3.id()),

                p -> p.size() == 2 && p.get(0).equals("1") && ((Path) p.get(1)).get(0).equals(v1.id())
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("depTime") && ((Map) ((Path) p.get(1)).get(1)).get("depTime").equals(17L)
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("arrTime") && ((Map) ((Path) p.get(1)).get(1)).get("arrTime").equals(20L)
                        && ((Path) p.get(1)).get(2).equals(v2.id()),

                p -> p.size() == 2 && p.get(0).equals("1") && ((Path) p.get(1)).get(0).equals(v1.id())
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("depTime") && ((Map) ((Path) p.get(1)).get(1)).get("depTime").equals(17L)
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("arrTime") && ((Map) ((Path) p.get(1)).get(1)).get("arrTime").equals(20L)
                        && ((Path) p.get(1)).get(2).equals(v2.id())
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("depTime") && ((Map) ((Path) p.get(1)).get(3)).get("depTime").equals(25L)
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("arrTime") && ((Map) ((Path) p.get(1)).get(3)).get("arrTime").equals(30L)
                        && ((Path) p.get(1)).get(4).equals(v3.id()),

                p -> p.size() == 2 && p.get(0).equals("1") && ((Path) p.get(1)).get(0).equals(v2.id())
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("depTime") && ((Map) ((Path) p.get(1)).get(1)).get("depTime").equals(25L)
                        && ((Map) ((Path) p.get(1)).get(1)).containsKey("arrTime") && ((Map) ((Path) p.get(1)).get(1)).get("arrTime").equals(30L)
                        && ((Path) p.get(1)).get(2).equals(v3.id())
        );
        for (Predicate<List<Object>> pathPredicate : pathsToAssert) {
            Optional<List<Object>> objectList = paths.stream().filter(pathPredicate).findAny();
            Assert.assertTrue(objectList.isPresent());
            Assert.assertTrue(paths.remove(objectList.get()));
        }
        Assert.assertTrue(paths.isEmpty());

        gp = query2(this.sqlgGraph.traversal());
        checkResult(gp);
    }


    @SuppressWarnings("unchecked")
    private DefaultGraphTraversal query1(GraphTraversalSource g) {
        Function timeAtWarehouse = o -> {
            Map m = (Map) o;
            Long dt = ((Edge) (m.get("curr"))).value("depTime");
            Long at = ((Edge) (m.get("prev"))).value("arrTime");
            return (dt - at) >= 0 ? (dt - at) : Long.MAX_VALUE;
        };

        Predicate checkNegativeTime = o -> {
            Map m = (Map) (((Traverser) o).get());
            Long dt = ((Edge) (m.get("curr"))).value("depTime");
            Long at = ((Edge) (m.get("prev"))).value("arrTime");
            return (dt - at) >= 0;
        };

        return (DefaultGraphTraversal)g.V().outE("tsw").as("e").inV().emit().repeat(
                __.flatMap(
                        __.outE("tsw").filter(__.as("edge").select(last, "e").where(P.eq("edge")).by("speed")).
                                group().by(__.inV()).by(__.project("curr", "prev").by().by(__.select(last, "e")).fold()).select(Column.values).unfold().
                                order(local).by(timeAtWarehouse).
                                limit(local, 1).
                                filter(checkNegativeTime).
                                select("curr")
                ).as("e").inV().simplePath()
        ).times(20).map(__.union(__.select(last, "e").by("speed"), (Traversal) __.path().by(T.id).by(__.valueMap("arrTime", "depTime"))).fold());
    }

    @SuppressWarnings({"RedundantCast", "unchecked"})
    private DefaultGraphTraversal query2(GraphTraversalSource g) {
        return (DefaultGraphTraversal)g.withSack(0).V().outE("tsw").as("e").inV().emit().repeat(
                __.flatMap(
                        __.outE("tsw").filter(__.as("edge").select(last, "e").where(P.eq("edge")).by("speed")).
                                group().by(__.inV()).
                                by(__.project("curr", "time").by().
                                        by(__.sack(Operator.assign).by("depTime").select(last, "e").sack(Operator.minus).by("arrTime").sack()).
                                        filter(__.select("time").is(P.gte(0))).fold()).
                                select(Column.values).unfold().order(local).by(__.select("time")).limit(local, 1).select("curr")
                ).as("e").inV().simplePath()
        ).times(20).map(__.union((Traversal) __.select(last, "e").by("speed"), (Traversal) __.path().by(T.id).by(__.valueMap("arrTime", "depTime"))).fold());
    }

    private void checkResult(GraphTraversal gp) {
        int count = 0;
        while (gp.hasNext()) {
            logger.info(gp.next().toString());
            count++;
        }
        Assert.assertEquals(8, count);
    }

}
