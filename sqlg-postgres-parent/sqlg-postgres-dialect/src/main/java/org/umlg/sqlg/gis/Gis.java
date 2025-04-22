package org.umlg.sqlg.gis;

import net.postgis.jdbc.geometry.Point;
import org.umlg.sqlg.structure.SqlgGraph;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by pieter on 2015/09/13.
 */
public class Gis {

    public static final int SRID = 4326;
    public static final Gis GIS = new Gis();
    private SqlgGraph sqlgGraph;

    private Gis() {
    }

    public double distanceBetween(Point johannesburg, Point pretoria) {
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            String sql = "SELECT ST_DistanceSphere(ST_GeomFromText('" + johannesburg.toString() + "'), ST_GeomFromText('" + pretoria.toString() + "'))";
            if (statement.execute(sql)) {
                ResultSet resultSet = statement.getResultSet();
                if (resultSet.next()) {
                    return resultSet.getDouble("st_distancesphere");
                } else {
                    return -1;
                }
            } else {
                return -1;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public double distanceBetween(GeographyPoint johannesburg, GeographyPoint pretoria) {
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            if (statement.execute("SELECT ST_Distance('" + johannesburg.toString() + "'::geography, '" + pretoria.toString() + "':: geography)")) {
                ResultSet resultSet = statement.getResultSet();
                if (resultSet.next()) {
                    return resultSet.getDouble("st_distance");
                } else {
                    return -1;
                }
            } else {
                return -1;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void setSqlgGraph(SqlgGraph sqlgGraph) {
        this.sqlgGraph = sqlgGraph;
    }
}
