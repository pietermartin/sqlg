package org.umlg.sqlg.test.gis;

import net.postgis.jdbc.geometry.Point;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.gis.Gis;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

public class TestBulkGis extends BaseTest {

    @Test
    public void testBulkPoint() {
        sqlgGraph.tx().normalBatchModeOn();
        Point johannesburgPoint = new Point(26.2044, 28.0456);
        Vertex johannesburg = this.sqlgGraph.addVertex(T.label, "Gis", "point", johannesburgPoint);
        Point pretoriaPoint = new Point(25.7461, 28.1881);
        Vertex pretoria = this.sqlgGraph.addVertex(T.label, "Gis", "point", pretoriaPoint);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(johannesburgPoint, this.sqlgGraph.traversal().V(johannesburg.id()).next().value("point"));
        Assert.assertEquals(pretoriaPoint, this.sqlgGraph.traversal().V(pretoria.id()).next().value("point"));
        Gis gis = this.sqlgGraph.gis();
        System.out.println(gis.distanceBetween(johannesburgPoint, pretoriaPoint));
        johannesburgPoint = new Point(26.2055, 28.0477);
        johannesburg.property("point", johannesburgPoint);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(johannesburgPoint, this.sqlgGraph.traversal().V(johannesburg.id()).next().value("point"));
    }

    @Test
    public void testBulkPointAndQueryBeforeCommit() {
        Point johannesburgPoint = new Point(26.2044, 28.0456);
        Vertex johannesburg = this.sqlgGraph.addVertex(T.label, "Gis", "point", johannesburgPoint, "name", "Johannesburg");
        Point pretoriaPoint = new Point(25.7461, 28.1881);
        Vertex pretoria = this.sqlgGraph.addVertex(T.label, "Gis", "point", pretoriaPoint, "name", "Pretoria");
        this.sqlgGraph.tx().commit();
        sqlgGraph.tx().normalBatchModeOn();
        pretoria.property("point", johannesburgPoint);
        johannesburg.property("point", pretoriaPoint);

        List<Vertex> johannesburgs=  this.sqlgGraph.traversal().V().hasLabel("Gis")
                .has("name", "Johannesburg")
                .toList();
        Assert.assertEquals(1, johannesburgs.size());
        johannesburg = johannesburgs.get(0);
        Assert.assertEquals(pretoriaPoint, johannesburg.value("point"));

        List<Vertex> pretorias =  this.sqlgGraph.traversal().V().hasLabel("Gis")
                .has("name", "Pretoria")
                .toList();
        Assert.assertEquals(1, pretorias.size());
        pretoria = pretorias.get(0);
        Assert.assertEquals(johannesburgPoint, pretoria.value("point"));

        this.sqlgGraph.tx().commit();
    }

}
