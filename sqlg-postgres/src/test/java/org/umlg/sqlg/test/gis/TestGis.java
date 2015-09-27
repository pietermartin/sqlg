package org.umlg.sqlg.test.gis;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.postgis.LinearRing;
import org.postgis.PGgeometry;
import org.postgis.Point;
import org.postgis.Polygon;
import org.umlg.sqlg.gis.GeographyPoint;
import org.umlg.sqlg.gis.GeographyPolygon;
import org.umlg.sqlg.gis.Gis;
import org.umlg.sqlg.test.BaseTest;

import java.sql.SQLException;

/**
 * Created by pieter on 2015/09/13.
 */
public class TestGis extends BaseTest {

    @Test
    public void testPoint() {
        Point johannesburgPoint = new Point(26.2044, 28.0456);
        Vertex johannesburg = this.sqlgGraph.addVertex(T.label, "Gis", "point", johannesburgPoint);
        Point pretoriaPoint = new Point(25.7461, 28.1881);
        Vertex pretoria = this.sqlgGraph.addVertex(T.label, "Gis", "point", pretoriaPoint);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(johannesburgPoint, this.sqlgGraph.traversal().V(johannesburg.id()).next().value("point"));
        Assert.assertEquals(pretoriaPoint, this.sqlgGraph.traversal().V(pretoria.id()).next().value("point"));
        Gis gis = this.sqlgGraph.gis();
        System.out.println(gis.distanceBetween(johannesburgPoint, pretoriaPoint));
    }

    @Test
    public void testGeographyPoint() {
        GeographyPoint geographyPointJohannesburg = new GeographyPoint(26.2044, 28.0456);
        Vertex johannesburg = this.sqlgGraph.addVertex(T.label, "Gis", "geographyPoint", geographyPointJohannesburg);
        GeographyPoint geographyPointPretoria = new GeographyPoint(25.7461, 28.1881);
        Vertex pretoria = this.sqlgGraph.addVertex(T.label, "Gis", "geographyPoint", geographyPointPretoria);
        this.sqlgGraph.tx().commit();
        Object geographyPoint = this.sqlgGraph.traversal().V(johannesburg.id()).next().value("geographyPoint");
        Assert.assertEquals(geographyPointJohannesburg, geographyPoint);
        geographyPoint = this.sqlgGraph.traversal().V(pretoria.id()).next().value("geographyPoint");
        Assert.assertEquals(geographyPointPretoria, geographyPoint);
        Gis gis = this.sqlgGraph.gis();
        System.out.println(gis.distanceBetween(geographyPointJohannesburg, geographyPointPretoria));
    }

    @Test
    public void testPolygon() throws SQLException {
        LinearRing linearRing = new LinearRing("0 0, 1 1, 1 2, 1 1, 0 0");
        Polygon polygon1 = new Polygon(new LinearRing[]{linearRing});
        Vertex johannesburg = this.sqlgGraph.addVertex(T.label, "Gis", "polygon", polygon1);
        this.sqlgGraph.tx().commit();
        Polygon polygon = this.sqlgGraph.traversal().V(johannesburg.id()).next().value("polygon");
        Assert.assertEquals(polygon1, polygon);
    }

    @Test
    public void testGeographyPolygon() throws SQLException {
        LinearRing linearRing = new LinearRing("0 0, 1 1, 1 2, 1 1, 0 0");
        GeographyPolygon polygon1 = new GeographyPolygon(new LinearRing[]{linearRing});
        Vertex johannesburg = this.sqlgGraph.addVertex(T.label, "Gis", "polygon", polygon1);
        this.sqlgGraph.tx().commit();
        GeographyPolygon geographyPolygon = this.sqlgGraph.traversal().V(johannesburg.id()).next().value("polygon");
        Assert.assertEquals(polygon1, geographyPolygon);
    }
}
