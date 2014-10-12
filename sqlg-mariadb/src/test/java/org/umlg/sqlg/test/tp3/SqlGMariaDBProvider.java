package org.umlg.sqlg.test.tp3;

import com.tinkerpop.gremlin.AbstractGraphProvider;
import com.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.Configuration;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.SqlgDataSource;

import java.beans.PropertyVetoException;
import java.lang.reflect.Constructor;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2014/07/13
 * Time: 5:57 PM
 */
public class SqlGMariaDBProvider extends AbstractGraphProvider {

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName) {
        return new HashMap<String, Object>() {{
            put("gremlin.graph", SqlgGraph.class.getName());
            put("jdbc.url", "jdbc:mysql://localhost:3306/");
            put("mariadb.db", graphName);
            put("jdbc.username", "sqlg");
            put("jdbc.password", "password");
        }};
    }

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        if (null != g) {
                g.tx().rollback();
            g.close();
        }
        SqlDialect sqlDialect;
        try {
            Class<?> sqlDialectClass = Class.forName("org.umlg.sqlg.sql.dialect.MariaDbDialect");
            Constructor<?> constructor = sqlDialectClass.getConstructor(Configuration.class);
            sqlDialect = (SqlDialect) constructor.newInstance(configuration);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            SqlgDataSource.INSTANCE.setupDataSource(
                    sqlDialect.getJdbcDriver(),
                    configuration.getString("jdbc.url"),
                    configuration.getString("jdbc.username"),
                    configuration.getString("jdbc.password"));
        } catch (PropertyVetoException e) {
            throw new RuntimeException(e);
        }
        try (Connection conn = SqlgDataSource.INSTANCE.get(configuration.getString("jdbc.url")).getConnection()) {
            DatabaseMetaData metadata = conn.getMetaData();
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
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }


}
