package org.umlg.sqlg.sql.dialect;

import org.umlg.sqlg.structure.SqlgExceptions;
import org.umlg.sqlg.structure.SqlgGraph;

/**
 * Date: 2016/09/03
 * Time: 4:09 PM
 */
public interface SqlSchemaChangeDialect extends SqlDialect {

    default void lock(SqlgGraph sqlgGraph) {
        throw SqlgExceptions.multipleJvmNotSupported(dialectName());
    }

    default void registerListener(SqlgGraph sqlgGraph) {
        throw SqlgExceptions.multipleJvmNotSupported(dialectName());
    }

    default void unregisterListener() {
        throw SqlgExceptions.multipleJvmNotSupported(dialectName());
    }

}
