package org.umlg.sqlgraph.sql.impl;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.umlg.sqlgraph.structure.SqlElement;
import org.umlg.sqlgraph.structure.SqlGraph;

import java.sql.*;
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

        StringBuilder sql = new StringBuilder("CREATE TABLE \"");
        sql.append(tableName);
        sql.append("\" (ID BIGINT NOT NULL");
        if (columns.size() > 0) {
            sql.append(", ");
        }
        int i = 1;
        for (ImmutablePair<String, SchemaManager.PropertyType> column : columns) {
            //Columns map 1 to 1 to property keys and are case sensitive
            sql.append("\"").append(column.left).append("\" ").append(column.right.getDbName());
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
        StringBuilder sql = new StringBuilder("CREATE TABLE \"");
        sql.append(tableName);
        sql.append("\" (ID BIGINT NOT NULL");
        if (columns.size() > 0) {
            sql.append(", ");
        }
        int i = 1;
        for (ImmutablePair<String, SchemaManager.PropertyType> column : columns) {
            //Columns map 1 to 1 to property keys and are case sensitive
            sql.append("\"").append(column.left).append("\" ").append(column.right.getDbName());
            if (i++ < columns.size()) {
                sql.append(", ");
            }
        }
        sql.append(", \"");
        sql.append(inTable);
        sql.append(SqlElement.IN_VERTEX_COLUMN_END);
        sql.append("\" LONG NOT NULL, \"");
        sql.append(outTable);
        sql.append(SqlElement.OUT_VERTEX_COLUMN_END);
        sql.append("\" LONG NOT NULL, ");
        sql.append("FOREIGN KEY (\"");
        sql.append(inTable);
        sql.append(SqlElement.IN_VERTEX_COLUMN_END);
        sql.append("\") REFERENCES \"");
        sql.append(inTable);
        sql.append("\" (ID), ");
        sql.append(" FOREIGN KEY (\"");
        sql.append(outTable);
        sql.append(SqlElement.OUT_VERTEX_COLUMN_END);
        sql.append("\") REFERENCES \"");
        sql.append(outTable);
        sql.append("\" (ID));");

        Connection conn = null;
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

    public boolean vericesExist() {
        return tableExist(SchemaManager.VERTICES);
    }

    public void createVerticesTable() {
        StringBuilder sql = new StringBuilder("CREATE TABLE ");
        sql.append(SchemaManager.VERTICES);
        sql.append("(ID BIGINT IDENTITY, VERTEX_TABLE VARCHAR);");
        Connection conn = null;
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

    public boolean tableExist(String table) {
        Connection conn;
        try {
            conn = this.sqlGraph.tx().getConnection();
            DatabaseMetaData metadata = conn.getMetaData();
            String catalog = null;
            String schemaPattern = null;
            String tableNamePattern = table;
            String[] types = null;

            ResultSet result = metadata.getTables(catalog, schemaPattern, tableNamePattern, types);
            while (result.next()) {
                return true;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
        }
        return false;
    }

    public boolean edgesExist() {
        return tableExist(SchemaManager.EDGES);
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
