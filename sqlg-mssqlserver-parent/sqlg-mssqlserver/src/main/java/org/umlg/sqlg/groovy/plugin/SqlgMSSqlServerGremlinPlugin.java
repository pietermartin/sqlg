package org.umlg.sqlg.groovy.plugin;

import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.structure.topology.*;

/**
 * Date: 2014/10/11
 * Time: 9:55 AM
 */
public class SqlgMSSqlServerGremlinPlugin  extends AbstractGremlinPlugin {

    private static final String NAME = "sqlg.mssqlserver";
    private static final ImportCustomizer imports;

    static {
        try {
            imports = DefaultImportCustomizer.build()
                    .addClassImports(
                            PropertyType.class,
                            RecordId.class,
                            SchemaTable.class,
                            SqlgEdge.class,
                            SqlgElement.class,
                            SqlgGraph.class,
                            SqlgProperty.class,
                            SqlgVertex.class,
                            SqlgVertexProperty.class,
                            Topology.class,
                            EdgeLabel.class,
                            VertexLabel.class,
                            Schema.class,
                            PropertyColumn.class,
                            Index.class,
                            IndexType.class,
                            Graph.class
                    )
                    .create();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static final SqlgMSSqlServerGremlinPlugin instance = new SqlgMSSqlServerGremlinPlugin();

    public SqlgMSSqlServerGremlinPlugin() {
        super(NAME, imports);
    }

    public static SqlgMSSqlServerGremlinPlugin instance() {
        return instance;
    }

    @Override
    public boolean requireRestart() {
        return true;
    }
}
