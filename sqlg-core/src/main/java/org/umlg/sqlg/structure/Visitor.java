package org.umlg.sqlg.structure;

import org.umlg.sqlg.sql.parse.SchemaTableTree;

/**
 * Created by pieter on 2015/01/25.
 */
@FunctionalInterface
public interface Visitor<R> {
    R visit(SchemaTableTree schemaTableTree);
}
