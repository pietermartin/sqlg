package org.umlg.sqlg.structure;

/**
 * Created by pieter on 2015/01/25.
 */
@FunctionalInterface
public interface Visitor<R> {
    R visit(SchemaTableTree schemaTableTree);
}
