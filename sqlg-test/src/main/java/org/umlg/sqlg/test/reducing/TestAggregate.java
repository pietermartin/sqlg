package org.umlg.sqlg.test.reducing;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2015/10/17
 * Time: 12:45 PM
 */
public class TestAggregate extends BaseTest {

    @Test
    public void testMean() {
        loadModern();
        Number mean = this.sqlgGraph.traversal().V().values("age").mean().next();
        if (isHsqldb()) {
            //hsqldb returns an int
            Assert.assertEquals(30, mean);
        } else {
            Assert.assertEquals(30.75, mean);
        }
    }

    @Test
    public void testAggregate() throws InterruptedException {
        loadModern(this.sqlgGraph);
        testAggregate_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testAggregate_assert(this.sqlgGraph1);
        }
    }

    private void testAggregate_assert(SqlgGraph sqlgGraph) {
        assertModernGraph(sqlgGraph, true, false);
        GraphTraversalSource g = sqlgGraph.traversal();
        Traversal<Vertex, Path> traversal = g.V().out().aggregate("a").path();
        printTraversalForm(traversal);
        int count = 0;
        final Map<String, Long> firstStepCounts = new HashMap<>();
        final Map<String, Long> secondStepCounts = new HashMap<>();
        while (traversal.hasNext()) {
            count++;
            final Path path = traversal.next();
            final String first = path.get(0).toString();
            final String second = path.get(1).toString();
            MatcherAssert.assertThat(first, CoreMatchers.not(second));
            MapHelper.incr(firstStepCounts, first, 1L);
            MapHelper.incr(secondStepCounts, second, 1L);
        }
        Assert.assertEquals(6, count);
        Assert.assertEquals(3, firstStepCounts.size());
        Assert.assertEquals(4, secondStepCounts.size());
        Assert.assertTrue(firstStepCounts.containsValue(3L));
        Assert.assertTrue(firstStepCounts.containsValue(2L));
        Assert.assertTrue(firstStepCounts.containsValue(1L));
        Assert.assertTrue(secondStepCounts.containsValue(3L));
        Assert.assertTrue(secondStepCounts.containsValue(1L));

    }
}
