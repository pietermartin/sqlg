package org.umlg.sqlg.test.tp3;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.SqlgPlugin;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.structure.topology.Topology;
import org.umlg.sqlg.util.SqlgUtil;

import java.sql.Connection;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2015/12/13
 */
@SuppressWarnings("rawtypes")
public abstract class SqlgAbstractGraphProvider extends AbstractGraphProvider {

    private static final Logger logger = LoggerFactory.getLogger(SqlgGraph.class.getName());

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
        logger.debug("clearing datasource " + configuration.getString("jdbc.url"));
        SqlgDataSource sqlgDataSource = null;
        if (null != g) {
            if (g.features().graph().supportsTransactions() && g.tx().isOpen()) {
                g.tx().rollback();
            }
            g.close();
        }
        SqlgPlugin plugin = getSqlgPlugin();
        SqlDialect sqlDialect = plugin.instantiateDialect();
        try {
            sqlgDataSource = SqlgDataSourceFactory.create(configuration);
            try (Connection conn = sqlgDataSource.getDatasource().getConnection()) {
                SqlgUtil.dropDb(sqlDialect, conn);
            }
        } finally {
            if (sqlgDataSource != null) {
                sqlgDataSource.close();
            }
        }
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATIONS;
    }

    @Override
    public Object convertId(final Object id, final Class<? extends Element> c) {
        return "jippo.jippo" + Topology.LABEL_SEPARATOR + id.toString();
    }

    public abstract SqlgPlugin getSqlgPlugin();
}
