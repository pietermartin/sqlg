package org.umlg.sqlg.sql.dialect;

import com.fasterxml.jackson.databind.JsonNode;
import org.umlg.sqlg.structure.SqlgExceptions;
import org.umlg.sqlg.structure.SqlgGraph;

import java.time.LocalDateTime;

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

    default int notifyChange(SqlgGraph sqlgGraph, LocalDateTime timestamp, JsonNode jsonNode) {
        throw SqlgExceptions.multipleJvmNotSupported(dialectName());
    }

}
