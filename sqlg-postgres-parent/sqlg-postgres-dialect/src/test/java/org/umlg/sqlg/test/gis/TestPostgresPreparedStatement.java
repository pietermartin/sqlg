package org.umlg.sqlg.test.gis;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2017/01/15
 * Time: 8:50 PM
 */
public class TestPostgresPreparedStatement extends BaseTest {

    @Test
    public void test() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "name", "halo");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.traversal().V(a).next();
        a.property("surname", "asdasd");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.traversal().V(a).next();
    }
}
