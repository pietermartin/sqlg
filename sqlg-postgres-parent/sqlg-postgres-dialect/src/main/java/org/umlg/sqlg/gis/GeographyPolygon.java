package org.umlg.sqlg.gis;

import net.postgis.jdbc.geometry.Geometry;
import net.postgis.jdbc.geometry.LinearRing;
import net.postgis.jdbc.geometry.Polygon;

import java.sql.SQLException;

/**
 * Created by pieter on 2015/09/13.
 */
public class GeographyPolygon extends Polygon {

    public GeographyPolygon(Polygon polygon) {
        this.srid = polygon.srid;
        this.haveMeasure = polygon.haveMeasure;
        this.dimension = polygon.dimension;
        this.subgeoms = new Geometry[polygon.numGeoms()];
        for (int i = 0 ; i < polygon.numGeoms(); i++) {
            subgeoms[i] = polygon.getSubGeometry(i);
        }
    }

    public GeographyPolygon() {
        this.srid = 4326;
    }

    public GeographyPolygon(LinearRing[] rings) {
        super(rings);
        this.srid = Gis.SRID;
    }

    public GeographyPolygon(String value) throws SQLException {
        super(value);
        this.srid = Gis.SRID;
    }

    public GeographyPolygon(String value, boolean haveM) throws SQLException {
        super(value, haveM);
        this.srid = Gis.SRID;
    }
}
