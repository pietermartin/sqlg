package org.umlg.sqlg.test.repeatstep;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/06/05
 */
public class NetAggregateTest extends BaseTest {

    @Test
    public void test2() {
        List<Path> vertices = this.sqlgGraph.traversal()
                .V()
                .hasLabel("TransmissionAtoB")
                .has("node", "X5069")
                .repeat(__.out().simplePath())
//                .until(__.or(__.loops().is(P.gt(15)), __.has("hubSite", true)))
                .until(__.has("hubSite", true))
                .path()
                .toList();
        System.out.println(vertices.size());
    }
}
