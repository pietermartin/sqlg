package org.umlg.sqlg.sql.dialect;

import org.umlg.sqlg.structure.SqlgExceptions;
import org.umlg.sqlg.structure.Topology;

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
}
