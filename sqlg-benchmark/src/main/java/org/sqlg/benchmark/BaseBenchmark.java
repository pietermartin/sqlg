package org.sqlg.benchmark;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.umlg.sqlg.structure.SqlgGraph;

import java.net.URL;

/**
 * Created by pieter on 2015/09/26.
 */
class BaseBenchmark {

    private Configuration getConfiguration() {
        try {
            URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
            Configuration configuration = new PropertiesConfiguration(sqlProperties);
            if (!configuration.containsKey("jdbc.url"))
                throw new IllegalArgumentException("SqlGraph configuration requires that the jdbc.url sqlg.property be set");
            return configuration;
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    SqlgGraph getSqlgGraph() {
        return getSqlgGraph(false);
    }

    SqlgGraph getSqlgGraph(boolean distributed) {
        Configuration configuration = getConfiguration();
        configuration.addProperty("distributed", distributed);
        return SqlgGraph.open(configuration);
    }

    void closeSqlgGraph(SqlgGraph sqlgGraph) {
        sqlgGraph.tx().onClose(Transaction.CLOSE_BEHAVIOR.ROLLBACK);
        try {
            sqlgGraph.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
