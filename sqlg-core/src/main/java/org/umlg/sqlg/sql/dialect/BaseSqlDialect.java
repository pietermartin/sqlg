package org.umlg.sqlg.sql.dialect;

import org.apache.commons.lang3.tuple.Triple;
import org.umlg.sqlg.structure.SqlgExceptions;
import org.umlg.sqlg.structure.Topology;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Date: 2014/08/21
 * Time: 6:52 PM
 */
public abstract class BaseSqlDialect implements SqlDialect, SqlBulkDialect, SqlSchemaChangeDialect {

    public BaseSqlDialect() {
    }

    public void validateColumnName(String column) {
        if (column.endsWith(Topology.IN_VERTEX_COLUMN_END) || column.endsWith(Topology.OUT_VERTEX_COLUMN_END)) {
            throw SqlgExceptions.invalidColumnName("Column names may not end with " + Topology.IN_VERTEX_COLUMN_END + " or " + Topology.OUT_VERTEX_COLUMN_END + ". column = " + column);
        }
    }

    @Override
    public List<String> getSchemaNames(DatabaseMetaData metaData) {
        List<String> schemaNames = new ArrayList<>();
        try {
            try (ResultSet schemaRs = metaData.getSchemas()) {
                while (schemaRs.next()) {
                    String schema = schemaRs.getString(1);
                    if (!this.getInternalSchemas().contains(schema)) {
                        schemaNames.add(schema);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return schemaNames;
    }


    @Override
    public List<Triple<String, String, String>> getVertexTables(DatabaseMetaData metaData) {
        List<Triple<String, String, String>> vertexTables = new ArrayList<>();
        String[] types = new String[]{"TABLE"};
        try {
            //load the vertices
            try (ResultSet vertexRs = metaData.getTables(null, null, "V_%", types)) {
                while (vertexRs.next()) {
                    String tblCat = vertexRs.getString(1);
                    String schema = vertexRs.getString(2);
                    String table = vertexRs.getString(3);

                    //verify the table name matches our pattern
                    if (!table.startsWith("V_")) {
                        continue;
                    }
                    vertexTables.add(Triple.of(tblCat, schema, table));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return vertexTables;
    }

    @Override
    public List<Triple<String, String, String>> getEdgeTables(DatabaseMetaData metaData) {
        List<Triple<String, String, String>> edgeTables = new ArrayList<>();
        String[] types = new String[]{"TABLE"};
        try {
            //load the edges without their properties
            try (ResultSet edgeRs = metaData.getTables(null, null, "E_%", types)) {
                while (edgeRs.next()) {
                    String edgCat = edgeRs.getString(1);
                    String schema = edgeRs.getString(2);
                    String table = edgeRs.getString(3);
                    //verify the table name matches our pattern
                    if (!table.startsWith("E_")) {
                        continue;
                    }
                    edgeTables.add(Triple.of(edgCat, schema, table));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return edgeTables;
    }


}
