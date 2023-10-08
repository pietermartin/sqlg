package org.umlg.sqlg.test.concat;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

public class TestConcat extends BaseTest {

    @Test
    public void g_hasLabelXsoftwareX_asXaX_valuesXnameX_concatXunsesX_concatXselectXaXvaluesXlangX() {
        loadModern();
        DefaultGraphTraversal<Vertex, String> traversal = (DefaultGraphTraversal<Vertex, String>) this.sqlgGraph.traversal().V()
                .hasLabel("software").as("a").values("name")
                .concat(" uses ")
                .concat(__.select("a").values("lang"));
        printTraversalForm(traversal);
        List<String> results = traversal.toList();
//        Then the result should be unordered
//      | result |
//      | lop uses java |
//      | ripple uses java |
        Assert.assertTrue(results.contains("lop uses java"));
        Assert.assertTrue(results.contains("ripple uses java"));
    }
}
