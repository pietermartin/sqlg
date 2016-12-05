package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.tinkerpop.gremlin.process.traversal.Pop.last;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.local;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outV;
import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/10/25
 * Time: 5:04 PM
 */
public class TestRepeatStepOnEdges extends BaseTest {

    private Logger logger = LoggerFactory.getLogger(TestRepeatStepOnEdges.class);

    @Test
    public void test() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();
        List<Edge> edges = this.sqlgGraph.traversal().E().repeat(outV().outE()).times(2).emit().toList();
        assertEquals(2, edges.size());
    }

    @Test
    public void testLabels() {
        Vertex v0 = this.sqlgGraph.addVertex("code", "0");
        Vertex v1 = this.sqlgGraph.addVertex("code", "1");
        Vertex v2 = this.sqlgGraph.addVertex("code", "2");
        Vertex v3 = this.sqlgGraph.addVertex("code", "3");
        v0.addEdge("tsw", v1, "speed", "1", "arrTime", 10L, "depTime", 5L);
        v1.addEdge("tsw", v2, "speed", "1", "arrTime", 15L, "depTime", 9L); //must be ignored in longest path
        v1.addEdge("tsw", v2, "speed", "1", "arrTime", 20L, "depTime", 17L); //must be used in longest path
        v2.addEdge("tsw", v3, "speed", "1", "arrTime", 30L, "depTime", 25L);
        this.sqlgGraph.tx().commit();

        GraphTraversal<Vertex, Vertex> gt = this.sqlgGraph.traversal().V().outE("tsw").as("e").inV();
        printTraversalForm(gt);
        List<Vertex> vertices = IteratorUtils.list(gt);
        System.out.println(vertices.size());
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

        GraphTraversal gp =  this.sqlgGraph.traversal().V().outE("tsw").as("e").inV().emit().repeat(
                __.outE("tsw").as("e").inV().simplePath()
        ).times(20);
        //noinspection unchecked
        assertEquals(10, IteratorUtils.list(gp).size());

        gp = query1(this.sqlgGraph.traversal());
        checkResult(gp);
        gp = query2(this.sqlgGraph.traversal());
        checkResult(gp);
    }


    @SuppressWarnings("unchecked")
    private GraphTraversal query1(GraphTraversalSource g) {
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

        return g.V().outE("tsw").as("e").inV().emit().repeat(
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
    private GraphTraversal query2(GraphTraversalSource g) {
        return g.withSack(0).V().outE("tsw").as("e").inV().emit().repeat(
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
        assertEquals(8, count);
    }

}
