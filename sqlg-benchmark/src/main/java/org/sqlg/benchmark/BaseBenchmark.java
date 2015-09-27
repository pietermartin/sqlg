package org.sqlg.benchmark;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.SqlgDataSource;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.JDBC;

import java.lang.reflect.Constructor;
import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by pieter on 2015/09/26.
 */
public class BaseBenchmark {


    private Configuration getConfiguration() {
        try {
            URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
            Configuration configuration = new PropertiesConfiguration(sqlProperties);
            if (!configuration.containsKey("jdbc.url"))
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));
            return configuration;
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    protected void dropDb() {
        Configuration configuration = getConfiguration();
        SqlgDataSource sqlgDataSource = null;
        SqlDialect sqlDialect;
        Class<?> sqlDialectClass = findSqlgDialect();
        try {
            Constructor<?> constructor = sqlDialectClass.getConstructor(Configuration.class);
            sqlDialect = (SqlDialect) constructor.newInstance(configuration);
            sqlgDataSource = SqlgDataSource.setupDataSource(
                    sqlDialect.getJdbcDriver(),
                    configuration);
            Connection conn;
            conn = sqlgDataSource.get(configuration.getString("jdbc.url")).getConnection();
            DatabaseMetaData metadata = conn.getMetaData();
            if (sqlDialect.supportsCascade()) {
                String catalog = null;
                String schemaPattern = null;
                String tableNamePattern = "%";
                String[] types = {"TABLE"};
                ResultSet result = metadata.getTables(catalog, schemaPattern, tableNamePattern, types);
                while (result.next()) {
                    String schema = result.getString(2);
                    String table = result.getString(3);
                    if (sqlDialect.getGisSchemas().contains(schema) || sqlDialect.getSpacialRefTable().contains(table)) {
                        continue;
                    }
                    StringBuilder sql = new StringBuilder("DROP TABLE ");
                    sql.append(sqlDialect.maybeWrapInQoutes(schema));
                    sql.append(".");
                    sql.append(sqlDialect.maybeWrapInQoutes(table));
                    sql.append(" CASCADE");
                    if (sqlDialect.needsSemicolon()) {
                        sql.append(";");
                    }
                    try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                        preparedStatement.executeUpdate();
                    }
                }
                catalog = null;
                schemaPattern = null;
                result = metadata.getSchemas(catalog, schemaPattern);
                while (result.next()) {
                    String schema = result.getString(1);
                    if (!sqlDialect.getDefaultSchemas().contains(schema)) {
                        StringBuilder sql = new StringBuilder("DROP SCHEMA ");
                        sql.append(sqlDialect.maybeWrapInQoutes(schema));
                        sql.append(" CASCADE");
                        if (sqlDialect.needsSemicolon()) {
                            sql.append(";");
                        }
                        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                            preparedStatement.executeUpdate();
                        }
                    }
                }
            } else if (!sqlDialect.supportSchemas()) {
                ResultSet result = metadata.getCatalogs();
                while (result.next()) {
                    StringBuilder sql = new StringBuilder("DROP DATABASE ");
                    String database = result.getString(1);
                    if (!sqlDialect.getDefaultSchemas().contains(database)) {
                        sql.append(sqlDialect.maybeWrapInQoutes(database));
                        if (sqlDialect.needsSemicolon()) {
                            sql.append(";");
                        }
                        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                            preparedStatement.executeUpdate();
                        }
                    }
                }
            } else {
                    conn.setAutoCommit(false);
                    JDBC.dropSchema(metadata, "APP");
                    conn.commit();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (sqlgDataSource != null)
                sqlgDataSource.close(configuration.getString("jdbc.url"));
        }
    }

    protected SqlgGraph getSqlgGraph() {
        return getSqlgGraph(false);
    }

    protected SqlgGraph getSqlgGraph(boolean distributed) {
        Configuration configuration = getConfiguration();
        configuration.addProperty("distributed", distributed);
        return SqlgGraph.open(configuration);
    }

    protected void closeSqlgGraph(SqlgGraph sqlgGraph) {
        sqlgGraph.tx().onClose(Transaction.CLOSE_BEHAVIOR.ROLLBACK);
        try {
            sqlgGraph.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Class<?> findSqlgDialect() {
        try {
            return Class.forName("org.umlg.sqlg.sql.dialect.PostgresDialect");
        } catch (ClassNotFoundException e) {
        }
        try {
            return Class.forName("org.umlg.sqlg.sql.dialect.MariaDbDialect");
        } catch (ClassNotFoundException e) {
        }
        try {
            return Class.forName("org.umlg.sqlg.sql.dialect.HsqldbDialect");
        } catch (ClassNotFoundException e) {
        }
        throw new IllegalStateException("No sqlg dialect found!");
    }
}
