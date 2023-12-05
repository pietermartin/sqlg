package org.umlg.sqlg.test.gis;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.postgis.Point;
import org.umlg.sqlg.gis.Gis;

/**
 * Created by pieter on 2015/09/13.
 */
//public class TestGis extends BaseTest {
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
        johannesburgPoint = new Point(26.2055, 28.0477);
        johannesburg.property("point", johannesburgPoint);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(johannesburgPoint, this.sqlgGraph.traversal().V(johannesburg.id()).next().value("point"));
    }

//    @Test
//    public void testLine() {
//        Point johannesburgPoint = new Point(26.2044, 28.0456);
//        Point pretoriaPoint = new Point(25.7461, 28.1881);
//        LineString lineString = new LineString(new Point[] {johannesburgPoint, pretoriaPoint});
//        Vertex pretoria = this.sqlgGraph.addVertex(T.label, "Gis", "lineString", lineString);
//        this.sqlgGraph.tx().commit();
//        Assert.assertEquals(lineString, this.sqlgGraph.traversal().V(pretoria.id()).next().value("lineString"));
//
//        johannesburgPoint = new Point(26.55, 30.0456);
//        pretoriaPoint = new Point(26.7461, 29.1881);
//        lineString = new LineString(new Point[] {johannesburgPoint, pretoriaPoint});
//        pretoria.property("lineString",lineString);
//        this.sqlgGraph.tx().commit();
//        Assert.assertEquals(lineString, this.sqlgGraph.traversal().V(pretoria.id()).next().value("lineString"));
//    }
//
//    @Test
//    public void testGeographyPoint() {
//        GeographyPoint geographyPointJohannesburg = new GeographyPoint(26.2044, 28.0456);
//        Vertex johannesburg = this.sqlgGraph.addVertex(T.label, "Gis", "geographyPoint", geographyPointJohannesburg);
//        GeographyPoint geographyPointPretoria = new GeographyPoint(25.7461, 28.1881);
//        Vertex pretoria = this.sqlgGraph.addVertex(T.label, "Gis", "geographyPoint", geographyPointPretoria);
//        this.sqlgGraph.tx().commit();
//        Object geographyPoint = this.sqlgGraph.traversal().V(johannesburg.id()).next().value("geographyPoint");
//        Assert.assertEquals(geographyPointJohannesburg, geographyPoint);
//        geographyPoint = this.sqlgGraph.traversal().V(pretoria.id()).next().value("geographyPoint");
//        Assert.assertEquals(geographyPointPretoria, geographyPoint);
//        Gis gis = this.sqlgGraph.gis();
//        System.out.println(gis.distanceBetween(geographyPointJohannesburg, geographyPointPretoria));
//
//        geographyPointPretoria = new GeographyPoint(25.7461, 28.1881);
//        pretoria.property("geographyPoint", geographyPointPretoria);
//
//        this.sqlgGraph.tx().commit();
//        geographyPoint = this.sqlgGraph.traversal().V(pretoria.id()).next().value("geographyPoint");
//        Assert.assertEquals(geographyPointPretoria, geographyPoint);
//    }
//
//    @Test
//    public void testPolygon() throws SQLException {
//        LinearRing linearRing = new LinearRing("0 0, 1 1, 1 2, 1 1, 0 0");
//        Polygon polygon1 = new Polygon(new LinearRing[]{linearRing});
//        Vertex johannesburg = this.sqlgGraph.addVertex(T.label, "Gis", "polygon", polygon1);
//        this.sqlgGraph.tx().commit();
//        Polygon polygon = this.sqlgGraph.traversal().V(johannesburg.id()).next().value("polygon");
//        Assert.assertEquals(polygon1, polygon);
//
//        linearRing = new LinearRing("0 0, 1 2, 2 2, 1 3, 0 0");
//        polygon1 = new Polygon(new LinearRing[]{linearRing});
//        johannesburg.property("polygon", polygon1);
//        this.sqlgGraph.tx().commit();
//        polygon = this.sqlgGraph.traversal().V(johannesburg.id()).next().value("polygon");
//        Assert.assertEquals(polygon1, polygon);
//    }
//
//    @Test
//    public void testGeographyPolygon() throws SQLException {
//        LinearRing linearRing = new LinearRing("0 0, 1 1, 1 2, 1 1, 0 0");
//        GeographyPolygon polygon1 = new GeographyPolygon(new LinearRing[]{linearRing});
//        Vertex johannesburg = this.sqlgGraph.addVertex(T.label, "Gis", "polygon", polygon1);
//        this.sqlgGraph.tx().commit();
//        GeographyPolygon geographyPolygon = this.sqlgGraph.traversal().V(johannesburg.id()).next().value("polygon");
//        Assert.assertEquals(polygon1, geographyPolygon);
//
//        linearRing = new LinearRing("0 1, 2 3, 1 3, 1 3, 0 1");
//        polygon1 = new GeographyPolygon(new LinearRing[]{linearRing});
//        johannesburg.property("polygon", polygon1);
//        this.sqlgGraph.tx().commit();
//        geographyPolygon = this.sqlgGraph.traversal().V(johannesburg.id()).next().value("polygon");
//        Assert.assertEquals(polygon1, geographyPolygon);
//    }
//
//    @Test
//    public void testUpgradePostGisTypes() throws Exception {
//        Point johannesburgPoint = new Point(26.2044, 28.0456);
//        Vertex johannesburg = this.sqlgGraph.addVertex(T.label, "Gis", "point", johannesburgPoint);
//
//        Point pretoriaPoint = new Point(25.7461, 28.1881);
//        LineString lineString = new LineString(new Point[] {johannesburgPoint, pretoriaPoint});
//        Vertex pretoria = this.sqlgGraph.addVertex(T.label, "Gis", "lineString", lineString);
//
//        GeographyPoint geographyPointJohannesburg = new GeographyPoint(26.2044, 28.0456);
//        Vertex johannesburgGeographyPoint = this.sqlgGraph.addVertex(T.label, "Gis", "geographyPoint", geographyPointJohannesburg);
//
//        LinearRing linearRing = new LinearRing("0 0, 1 1, 1 2, 1 1, 0 0");
//        Polygon polygon1 = new Polygon(new LinearRing[]{linearRing});
//        Vertex johannesburgPolygon = this.sqlgGraph.addVertex(T.label, "Gis", "polygon", polygon1);
//
//        linearRing = new LinearRing("0 0, 1 1, 1 2, 1 1, 0 0");
//        GeographyPolygon geographyPolygon = new GeographyPolygon(new LinearRing[]{linearRing});
//        Vertex johannesburgGeographyPolygon = this.sqlgGraph.addVertex(T.label, "Gis", "geographyPolygon", geographyPolygon);
//
//        this.sqlgGraph.tx().commit();
//        //Delete the topology
//        Connection conn = this.sqlgGraph.tx().getConnection();
//        try (Statement statement = conn.createStatement()) {
//            statement.execute("DROP SCHEMA " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("sqlg_schema") + " CASCADE");
//        }
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.close();
//        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
//            List<Vertex> schemaVertices = sqlgGraph1.topology().V().hasLabel("sqlg_schema.schema").has("name", sqlgGraph1.getSqlDialect().getPublicSchema()).toList();
//            Assert.assertEquals(1, schemaVertices.size());
//            List<Vertex> propertyVertices = sqlgGraph1.topology().V().hasLabel("sqlg_schema.schema").has("name", sqlgGraph1.getSqlDialect().getPublicSchema())
//                    .out("schema_vertex")
//                    .has("name", "Gis")
//                    .out("vertex_property")
//                    .has("name", "point")
//                    .toList();
//            Assert.assertEquals(1, propertyVertices.size());
//            Assert.assertEquals("POINT", propertyVertices.get(0).value("type"));
//
//            Assert.assertEquals(johannesburgPoint, sqlgGraph1.traversal().V(johannesburg.id()).next().value("point"));
//            Assert.assertEquals(lineString, sqlgGraph1.traversal().V(pretoria.id()).next().value("lineString"));
//            Assert.assertEquals(geographyPointJohannesburg, sqlgGraph1.traversal().V(johannesburgGeographyPoint.id()).next().value("geographyPoint"));
//            Assert.assertEquals(polygon1, sqlgGraph1.traversal().V(johannesburgPolygon.id()).next().value("polygon"));
//            Assert.assertEquals(geographyPolygon, sqlgGraph1.traversal().V(johannesburgGeographyPolygon.id()).next().value("geographyPolygon"));
//        }
//    }
}
