package org.umlg.sqlg.test.tp3;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.*;

import java.beans.PropertyVetoException;
import java.sql.*;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by pieter on 2015/12/13.
 */
public abstract class SqlgAbstractGraphProvider extends AbstractGraphProvider {

    private static final Set<Class> IMPLEMENTATIONS = new HashSet<Class>() {{
        add(SqlgEdge.class);
        add(SqlgElement.class);
        add(SqlgGraph.class);
        add(SqlgProperty.class);
        add(SqlgVertex.class);
        add(SqlgVertexProperty.class);
        add(DefaultGraphTraversal.class);
    }};

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        SqlgDataSource sqlgDataSource = null;
        if (null != g) {
            if (g.features().graph().supportsTransactions() && g.tx().isOpen()) {
                g.tx().rollback();
            }
            g.close();
        }
        SqlDialect sqlDialect = getSqlgDialect(configuration);
        try {
            sqlgDataSource = SqlgDataSource.setupDataSource(sqlDialect.getJdbcDriver(), configuration);
            try (Connection conn = sqlgDataSource.get(configuration.getString("jdbc.url")).getConnection()) {
                DatabaseMetaData metadata = conn.getMetaData();
                if (sqlDialect.supportsCascade()) {
                    String tableNamePattern = "%";
                    String[] types = {"TABLE"};
                    ResultSet resultSet = metadata.getTables(null, null, tableNamePattern, types);
                    while (resultSet.next()) {
                        String schema = resultSet.getString(2);
                        if (!sqlDialect.getGisSchemas().contains(schema)) {
                            StringBuilder sql = new StringBuilder("DROP TABLE ");
                            sql.append(sqlDialect.maybeWrapInQoutes(resultSet.getString(2)));
                            sql.append(".");
                            sql.append(sqlDialect.maybeWrapInQoutes(resultSet.getString(3)));
                            sql.append(" CASCADE");
                            if (sqlDialect.needsSemicolon()) {
                                sql.append(";");
                            }
                            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                                preparedStatement.executeUpdate();
                            }
                        }
                    }
                    resultSet = metadata.getSchemas(null, null);
                    while (resultSet.next()) {
                        String schema = resultSet.getString(1);
                        if (!sqlDialect.getDefaultSchemas().contains(schema) && !sqlDialect.getGisSchemas().contains(schema)) {
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
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } catch (PropertyVetoException e) {
            throw new RuntimeException(e);
        } finally {
            if (sqlgDataSource != null) {
                sqlgDataSource.close(configuration.getString("jdbc.url"));
            }
        }
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATIONS;
    }

    @Override
    public Object convertId(final Object id, final Class<? extends Element> c) {
        return "jippo.jippo" + SchemaManager.LABEL_SEPARATOR + id.toString();
    }

    public abstract SqlDialect getSqlgDialect(Configuration configuration);
}
