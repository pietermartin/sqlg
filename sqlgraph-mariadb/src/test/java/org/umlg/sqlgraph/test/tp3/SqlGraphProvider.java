package org.umlg.sqlgraph.test.tp3;

import com.tinkerpop.gremlin.AbstractGraphProvider;
import com.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.Configuration;
import org.umlg.sqlgraph.sql.dialect.SqlGraphDialect;
import org.umlg.sqlgraph.structure.SqlGraph;
import org.umlg.sqlgraph.structure.SqlGraphDataSource;

import java.beans.PropertyVetoException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2014/07/13
 * Time: 5:57 PM
 */
public class SqlGraphProvider extends AbstractGraphProvider {

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName) {
        return new HashMap<String, Object>() {{
            put("gremlin.graph", SqlGraph.class.getName());
            put("jdbc.driver", "org.mariadb.jdbc.Driver");
            put("jdbc.url", "jdbc:mysql://localhost:3306/" + graphName);
            put("jdbc.username", "test");
            put("jdbc.password", "password");
        }};
    }

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        if (null != g) {
            if (g.getFeatures().graph().supportsTransactions())
                g.tx().rollback();
            g.close();
        }
        SqlGraphDialect sqlGraphDialect = null;
        try {
            Class.forName(SqlGraphDialect.MARIADBDB.getJdbcDriver());
            sqlGraphDialect = SqlGraphDialect.MARIADBDB;
        } catch (ClassNotFoundException e) {
        }
        if (sqlGraphDialect == null) {
            throw new IllegalStateException("Maria db driver " + SqlGraphDialect.MARIADBDB.getJdbcDriver() + " must be on the classpath!");
        }
        try {
            SqlGraphDataSource.INSTANCE.setupDataSource(
                    sqlGraphDialect.getJdbcDriver(),
                    configuration.getString("jdbc.url"),
                    configuration.getString("jdbc.username"),
                    configuration.getString("jdbc.password"));
        } catch (PropertyVetoException e) {
            throw new RuntimeException(e);
        }
        try (Connection conn = SqlGraphDataSource.INSTANCE.get(configuration.getString("jdbc.url")).getConnection()) {
            DatabaseMetaData metadata = conn.getMetaData();
            String jdbcUrl = configuration.getString("jdbc.url");
            String catalog = jdbcUrl.substring(jdbcUrl.lastIndexOf("/") + 1);
            String schemaPattern = null;
            String tableNamePattern = "%";
            String[] types = {"TABLE"};
            ResultSet result = metadata.getTables(catalog, schemaPattern, tableNamePattern, types);
            while (result.next()) {
                StringBuilder sql = new StringBuilder("DROP TABLE ");
                sql.append(sqlGraphDialect.getSqlDialect().maybeWrapInQoutes(result.getString(3)));
                sql.append(" CASCADE;");
                try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                    preparedStatement.executeUpdate();
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }


}
