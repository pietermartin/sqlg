package org.umlg.sqlg.structure;

import org.umlg.sqlg.sql.parse.SchemaTableTree;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * Created by pieter on 2015/11/07.
 */
public class Dummy extends SqlgElement {

    public Dummy() {
    }

    public SchemaTable getSchemaTablePrefixed() {
        return null;
    }

    @Override
    protected void load() {

    }

    @Override
    public void loadResultSet(ResultSet resultSet, Map<String, Integer> columnNameCountMap, SchemaTableTree schemaTableTree) throws SQLException {

    }

    @Override
    public void loadLabeledResultSet(ResultSet resultSet, Map<String, Integer> columnMap, SchemaTableTree schemaTableTree) throws SQLException {

    }

    @Override
    public void loadResultSet(ResultSet resultSet) throws SQLException {

    }

    @Override
    public boolean equals(final Object object) {
        return object == this;
    }

    @Override
    public int hashCode() {
        return -1;
    }
}
