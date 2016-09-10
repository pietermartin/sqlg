package org.umlg.sqlg.sql.dialect;

import org.umlg.sqlg.structure.SchemaManager;
import org.umlg.sqlg.structure.SqlgExceptions;

/**
 * Date: 2014/08/21
 * Time: 6:52 PM
 */
public abstract class BaseSqlDialect implements SqlDialect, SqlBulkDialect, SqlSchemaChangeDialect {

    public BaseSqlDialect() {
    }

    public void validateColumnName(String column) {
        if (column.endsWith(SchemaManager.IN_VERTEX_COLUMN_END) || column.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END)) {
            throw SqlgExceptions.invalidColumnName("Column names may not end with " + SchemaManager.IN_VERTEX_COLUMN_END + " or " + SchemaManager.OUT_VERTEX_COLUMN_END + ". column = " + column);
        }
    }
}
