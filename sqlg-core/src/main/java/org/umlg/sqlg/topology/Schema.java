package org.umlg.sqlg.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.TopologyManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.umlg.sqlg.structure.SchemaManager.*;

/**
 * Date: 2016/09/04
 * Time: 8:49 AM
 */
public class Schema {

    private static Logger logger = LoggerFactory.getLogger(Schema.class.getName());
    private String name;
    private Map<String, VertexLabel> vertexLabels = new HashMap<>();
    private Map<String, VertexLabel> uncommittedVertexLabels = new HashMap<>();

    public static Schema createSchema(SqlgGraph sqlgGraph, String name) {
        Schema schema = new Schema(name);
        if (!sqlgGraph.getSqlDialect().getPublicSchema().equals(name)) {
            schema.createSchemaOnDb(sqlgGraph);
            TopologyManager.addSchema(sqlgGraph, name);
        }
        return schema;
    }

    private Schema(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public boolean existVertexLabel(String vertexLabel) {
        return this.vertexLabels.containsKey(vertexLabel) || this.uncommittedVertexLabels.containsKey(vertexLabel);
    }

    public VertexLabel getVertexLabel(String vertexLabelName) {
        VertexLabel vertexLabel = this.vertexLabels.get(vertexLabelName);
        if (vertexLabel != null) {
            return vertexLabel;
        } else {
            return this.uncommittedVertexLabels.get(vertexLabelName);
        }
    }

    public VertexLabel createVertexLabel(SqlgGraph sqlgGraph, String prefixedTable, ConcurrentHashMap<String, PropertyType> columns) {
        VertexLabel vertexLabel = VertexLabel.createVertexLabel(sqlgGraph, this, prefixedTable, columns);
        this.uncommittedVertexLabels.put(prefixedTable, vertexLabel);
        return vertexLabel;
    }

    public VertexLabel addMetaVertexLabel(String prefixedTable, ConcurrentHashMap<String, PropertyType> columns) {
        VertexLabel vertexLabel = VertexLabel.metaAddVertexLabel(this, prefixedTable, columns);
        this.vertexLabels.put(prefixedTable, vertexLabel);
        return vertexLabel;
    }

    private void createSchemaOnDb(SqlgGraph sqlgGraph) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE SCHEMA ");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.name));
        if (sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static Schema createMetaSchema(String name) {
        final Schema schema = new Schema(name);
        schema.addMetaVertexLabel("V_" + SQLG_SCHEMA_SCHEMA, new ConcurrentHashMap<>());
        schema.addMetaVertexLabel("V_" + SQLG_SCHEMA_VERTEX_LABEL, new ConcurrentHashMap<>());
        schema.addMetaVertexLabel("V_" + SQLG_SCHEMA_EDGE_LABEL, new ConcurrentHashMap<>());
        schema.addMetaVertexLabel("V_" + SQLG_SCHEMA_PROPERTY, new ConcurrentHashMap<>());
        return schema;
    }

    public void afterCommit() {

        for (Iterator<Map.Entry<String, VertexLabel>> it = this.uncommittedVertexLabels.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, VertexLabel> entry = it.next();
            this.vertexLabels.put(entry.getKey(), entry.getValue());
            entry.getValue().afterCommit();
            it.remove();
        }

    }

    public void afterRollback() {

        for (Iterator<Map.Entry<String, VertexLabel>> it = this.uncommittedVertexLabels.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, VertexLabel> entry = it.next();
            entry.getValue().afterRollback();
            it.remove();
        }

    }
}
