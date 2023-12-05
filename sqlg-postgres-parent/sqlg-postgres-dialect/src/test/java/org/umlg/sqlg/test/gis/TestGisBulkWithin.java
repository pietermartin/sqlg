package org.umlg.sqlg.test.gis;

/**
 * @author <a href="https://github.com/pietermartin">Pieter Martin</a>
 * Date: 2017/07/23
 */
public class TestGisBulkWithin {
//    public class TestGisBulkWithin extends BaseTest {

//    @Test
//    public void testBulkWithinPoint() {
//        Point point1 = new Point(26.2044, 28.0456);
//        Point point2 = new Point(26.2045, 28.0457);
//        Point point3 = new Point(26.2046, 28.0458);
//        Point point4 = new Point(26.2047, 28.0459);
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Gis", "point", point1);
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Gis", "point", point2);
//        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Gis", "point", point3);
//        Vertex v4 = this.sqlgGraph.addVertex(T.label, "Gis", "point", point4);
//        this.sqlgGraph.tx().commit();
//        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Gis").has("point", P.within(point1, point3, point4)).toList();
//        Assert.assertEquals(3, vertices.size());
//        Assert.assertTrue(Arrays.asList(v1, v3, v4).containsAll(vertices));
//    }
//
//    @Test
//    public void testBulkWithinLineString() {
//        Point point1 = new Point(26.2044, 28.0456);
//        Point point2 = new Point(26.2045, 28.0457);
//        LineString lineString1 = new LineString(new Point[] {point1, point2});
//
//        Point point3 = new Point(26.2046, 28.0458);
//        LineString lineString2 = new LineString(new Point[] {point1, point3});
//        LineString lineString3 = new LineString(new Point[] {point2, point3});
//
//        Point point4 = new Point(26.2047, 28.0459);
//        LineString lineString4 = new LineString(new Point[] {point1, point4});
//
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Gis", "line", lineString1);
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Gis", "line", lineString2);
//        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Gis", "line", lineString3);
//        Vertex v4 = this.sqlgGraph.addVertex(T.label, "Gis", "line", lineString4);
//        this.sqlgGraph.tx().commit();
//        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Gis").has("line", P.within(lineString1, lineString3, lineString4)).toList();
//        Assert.assertEquals(3, vertices.size());
//        Assert.assertTrue(Arrays.asList(v1, v3, v4).containsAll(vertices));
//    }
//
//    @Test
//    public void testBulkWithinPolygon() throws SQLException {
//        LinearRing linearRing1 = new LinearRing("0 0, 1 1, 1 2, 1 1, 0 0");
//        Polygon polygon1 = new Polygon(new LinearRing[]{linearRing1});
//        LinearRing linearRing2 = new LinearRing("1 1, 1 1, 1 2, 1 1, 1 1");
//        Polygon polygon2 = new Polygon(new LinearRing[]{linearRing2});
//        LinearRing linearRing3 = new LinearRing("2 2, 1 1, 1 2, 1 1, 2 2");
//        Polygon polygon3 = new Polygon(new LinearRing[]{linearRing3});
//        LinearRing linearRing4 = new LinearRing("1 3, 1 2, 2 2, 1 1, 1 3");
//        Polygon polygon4 = new Polygon(new LinearRing[]{linearRing4});
//
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Gis", "polygon", polygon1);
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Gis", "polygon", polygon2);
//        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Gis", "polygon", polygon3);
//        Vertex v4 = this.sqlgGraph.addVertex(T.label, "Gis", "polygon", polygon4);
//        this.sqlgGraph.tx().commit();
//        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Gis").has("polygon", P.within(polygon1, polygon3, polygon4)).toList();
//        Assert.assertEquals(3, vertices.size());
//        Assert.assertTrue(Arrays.asList(v1, v3, v4).containsAll(vertices));
//    }
//
//    @Test
//    public void testBulkWithinGeographyPoint() {
//        GeographyPoint point1 = new GeographyPoint(26.2044, 28.0456);
//        GeographyPoint point2 = new GeographyPoint(26.2045, 28.0457);
//        GeographyPoint point3 = new GeographyPoint(26.2046, 28.0458);
//        GeographyPoint point4 = new GeographyPoint(26.2047, 28.0459);
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Gis", "point", point1);
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Gis", "point", point2);
//        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Gis", "point", point3);
//        Vertex v4 = this.sqlgGraph.addVertex(T.label, "Gis", "point", point4);
//        this.sqlgGraph.tx().commit();
//        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Gis").has("point", P.within(point1, point3, point4)).toList();
//        Assert.assertEquals(3, vertices.size());
//        Assert.assertTrue(Arrays.asList(v1, v3, v4).containsAll(vertices));
//    }
//
//    @Test
//    public void testBulkWithinGeographyPolygon() throws SQLException {
//        LinearRing linearRing1 = new LinearRing("0 0, 1 1, 1 2, 1 1, 0 0");
//        GeographyPolygon polygon1 = new GeographyPolygon(new LinearRing[]{linearRing1});
//        LinearRing linearRing2 = new LinearRing("1 1, 1 1, 1 2, 1 1, 1 1");
//        GeographyPolygon polygon2 = new GeographyPolygon(new LinearRing[]{linearRing2});
//        LinearRing linearRing3 = new LinearRing("2 2, 1 1, 1 2, 1 1, 2 2");
//        GeographyPolygon polygon3 = new GeographyPolygon(new LinearRing[]{linearRing3});
//        LinearRing linearRing4 = new LinearRing("1 3, 1 2, 2 2, 1 1, 1 3");
//        GeographyPolygon polygon4 = new GeographyPolygon(new LinearRing[]{linearRing4});
//
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Gis", "polygon", polygon1);
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Gis", "polygon", polygon2);
//        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Gis", "polygon", polygon3);
//        Vertex v4 = this.sqlgGraph.addVertex(T.label, "Gis", "polygon", polygon4);
//        this.sqlgGraph.tx().commit();
//        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Gis").has("polygon", P.within(polygon1, polygon3, polygon4)).toList();
//        Assert.assertEquals(3, vertices.size());
//        Assert.assertTrue(Arrays.asList(v1, v3, v4).containsAll(vertices));
//    }
}
