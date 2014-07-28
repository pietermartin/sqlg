package org.umlg.sqlgraph.test.tp3;

import com.tinkerpop.gremlin.AbstractGraphProvider;
import com.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.Configuration;
import org.umlg.sqlgraph.sql.dialect.SqlDialect;
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
            put("sql.dialect", "org.umlg.sqlgraph.sql.dialect.MariaDBDialect");
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
        SqlDialect sqlDialect;
        try {
            Class<?> sqlDialectClass = Class.forName(configuration.getString("sql.dialect"));
            sqlDialect = (SqlDialect)sqlDialectClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            SqlGraphDataSource.INSTANCE.setupDataSource(
                    sqlDialect.getJdbcDriver(),
                    configuration.getString("jdbc.url"),
                    configuration.getString("jdbc.username"),
                    configuration.getString("jdbc.password"));
        } catch (PropertyVetoException e) {
            throw new RuntimeException(e);
        }
        try (Connection conn = SqlGraphDataSource.INSTANCE.get(configuration.getString("jdbc.url")).getConnection()) {
            DatabaseMetaData metadata = conn.getMetaData();
            if (sqlDialect.supportsCascade()) {
                String catalog = "sqlgraphdb";
                String schemaPattern = null;
                String tableNamePattern = "%";
                String[] types = {"TABLE"};
                ResultSet result = metadata.getTables(catalog, schemaPattern, tableNamePattern, types);
                while (result.next()) {
                    StringBuilder sql = new StringBuilder("DROP TABLE ");
                    sql.append(sqlDialect.maybeWrapInQoutes(result.getString(3)));
                    sql.append(" CASCADE");
                    if (sqlDialect.needsSemicolon()) {
                        sql.append(";");
                    }
                    try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                        preparedStatement.executeUpdate();
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }


}
