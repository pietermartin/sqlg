package org.umlg.sqlg.structure;

import com.google.common.collect.Multimap;
import org.umlg.sqlg.sql.parse.SchemaTableTree;

import java.sql.ResultSet;
import java.sql.SQLException;

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
    public void loadResultSet(ResultSet resultSet, SchemaTableTree schemaTableTree) throws SQLException {

    }

    @Override
    public void loadLabeledResultSet(ResultSet resultSet, Multimap<String, Integer> columnMap, SchemaTableTree schemaTableTree) throws SQLException {

    }

    @Override
    public void loadResultSet(ResultSet resultSet) throws SQLException {

    }

    @Override
    public int hashCode() {
        return -1;
    }
}
