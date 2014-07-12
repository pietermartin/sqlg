package org.umlg.sqlgraph.sql.impl;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.umlg.sqlgraph.structure.SqlGraph;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Date: 2014/07/12
 * Time: 1:25 PM
 */
public class SchemaCreator {

    public static final SchemaCreator INSTANCE = new SchemaCreator();
    private SqlGraph sqlGraph;

    private SchemaCreator() {
    }

    public void setSqlGraph(SqlGraph sqlGraph) {
        this.sqlGraph = sqlGraph;
    }

    public void createVertexTable(String tableName, List<ImmutablePair<String, SchemaManager.PropertyType>> columns) {

        StringBuilder sql = new StringBuilder("CREATE TABLE ");
        sql.append(tableName.toUpperCase());
        sql.append("(ID BIGINT NOT NULL");
        if (columns.size() > 0) {
            sql.append(", ");
        }
        int i = 1;
        for (ImmutablePair<String, SchemaManager.PropertyType> column : columns) {
            sql.append(column.left).append(" ").append(column.right.getDbName());
            if (i++ < columns.size()) {
                sql.append(", ");
            }
        }
        sql.append(");");
        Connection conn;
        Statement stmt = null;
        try {
            conn = this.sqlGraph.tx().getConnection();
            stmt = conn.createStatement();
            stmt.execute(sql.toString());
            stmt.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException se2) {
            }
        }
    }

    public void createEdgeTable(String tableName, String inTable, String outTable, List<ImmutablePair<String, SchemaManager.PropertyType>> columns) {
        StringBuilder sql = new StringBuilder("CREATE TABLE ");
        sql.append(tableName.toUpperCase());
        sql.append("(ID BIGINT NOT NULL");
        if (columns.size() > 0) {
            sql.append(", ");
        }
        int i = 1;
        for (ImmutablePair<String, SchemaManager.PropertyType> column : columns) {
            sql.append(column.left).append(" ").append(column.right.getDbName());
            if (i++ < columns.size()) {
                sql.append(", ");
            }
        }
        sql.append(", ");
        sql.append(inTable.toUpperCase());
        sql.append("_INID LONG NOT NULL, ");
        sql.append(outTable.toUpperCase());
        sql.append("_OUTID LONG NOT NULL, ");
        sql.append("FOREIGN KEY (");
        sql.append(inTable.toUpperCase());
        sql.append("_INID) REFERENCES ");
        sql.append(inTable.toUpperCase());
        sql.append("(ID), ");
        sql.append(" FOREIGN KEY (");
        sql.append(outTable.toUpperCase());
        sql.append("_OUTID) REFERENCES ");
        sql.append(outTable.toUpperCase());
        sql.append("(ID));");

        Connection conn;
        Statement stmt = null;
        try {
            conn = this.sqlGraph.tx().getConnection();
            stmt = conn.createStatement();
            stmt.execute(sql.toString());
            stmt.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException se2) {
            }
        }
    }

    public void createVerticesTable() {
        StringBuilder sql = new StringBuilder("CREATE TABLE ");
        sql.append(SchemaManager.VERTICES);
        sql.append("(ID BIGINT IDENTITY, VERTEX_TABLE VARCHAR);");
        Connection conn;
        Statement stmt = null;
        try {
            conn = this.sqlGraph.tx().getConnection();
            stmt = conn.createStatement();
            stmt.execute(sql.toString());
            stmt.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException se2) {
            }
        }
    }

    public void createEdgesTable() {
        StringBuilder sql = new StringBuilder("CREATE TABLE ");
        sql.append(SchemaManager.EDGES);
        sql.append("(ID BIGINT IDENTITY, EDGE_TABLE VARCHAR);");
        Connection conn;
        Statement stmt = null;
        try {
            conn = this.sqlGraph.tx().getConnection();
            stmt = conn.createStatement();
            stmt.execute(sql.toString());
            stmt.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException se2) {
            }
        }
    }

}
