package org.umlg.sqlg.test.tpfeature;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

public class TestTinkerPopFeatureTests extends BaseTest {

//    @Test
//    public void testBothCount() {
//        loadModern();
//        GraphTraversal<Vertex, String> traversal = this.sqlgGraph.traversal().V().both().both().order().by("name", Order.desc).dedup().by(__.outE().count()).<String>values("name");
//        printTraversalForm(traversal);
//        List<String> values = traversal.toList();
//        for (String value : values) {
//            System.out.println(value);
//        }
//    }

    @Test
    public void testRepeatOutWithTimesAndLimit() {
        loadModern();
//        List<Path> paths = sqlgGraph.traversal().V().repeat(__.out().repeat(__.out()).times(1)).times(1).path().by("name").toList();
        List<Path> paths = sqlgGraph.traversal().V().repeat(__.out().repeat(__.out().order().by("name", Order.desc)).times(1)).times(1).path().by("name").toList();
        for (Path path : paths) {
            System.out.println(path);
        }
    }
}
