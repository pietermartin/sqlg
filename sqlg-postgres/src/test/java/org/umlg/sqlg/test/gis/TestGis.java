package org.umlg.sqlg.test.gis;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.postgis.PGgeometry;
import org.postgis.Point;
import org.umlg.sqlg.test.BaseTest;

/**
 * Created by pieter on 2015/09/13.
 */
public class TestGis extends BaseTest {

    @Test
    public void testPoint() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Gis", "point", new Point(1,1));
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(new Point(1,1), this.sqlgGraph.traversal().V(v1.id()).next().value("point"));
    }
}
