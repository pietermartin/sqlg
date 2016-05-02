package org.sqlg.benchmark;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgUtil;

import java.net.URL;

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
        SqlgGraph sqlgGraph = getSqlgGraph();
        SqlgUtil.dropDb(sqlgGraph);
        closeSqlgGraph(sqlgGraph);
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
