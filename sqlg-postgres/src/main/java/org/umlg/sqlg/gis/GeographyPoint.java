package org.umlg.sqlg.gis;

import org.postgis.Point;

import java.sql.SQLException;

/**
 * Created by pieter on 2015/09/13.
 */
public class GeographyPoint extends Point {

    private Point point;

    public GeographyPoint(Point point) {
        this.x = point.x;
        this.y = point.y;
        this.z = point.z;
        this.dimension = point.dimension;
        this.m = point.m;
        this.srid = point.srid;
        this.haveMeasure = point.haveMeasure;
    }

    public GeographyPoint() {
        this.srid = 4326;
    }

    public GeographyPoint(double x, double y, double z) {
        super(x, y, z);
        this.srid = Gis.SRID;
    }

    public GeographyPoint(double x, double y) {
        super(x, y);
        this.srid = Gis.SRID;
    }

    public GeographyPoint(String value) throws SQLException {
        super(value);
        this.srid = Gis.SRID;
    }

    public GeographyPoint(String value, boolean haveM) throws SQLException {
        super(value, haveM);
        this.srid = Gis.SRID;
    }
}
