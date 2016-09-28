package org.umlg.sqlg.topology;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.TopologyManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static org.umlg.sqlg.structure.SchemaManager.*;

/**
 * Date: 2016/09/04
 * Time: 8:49 AM
 */
public class VertexLabel extends AbstractElement {

    private Logger logger = LoggerFactory.getLogger(VertexLabel.class.getName());
    //This just won't stick in my brain.
    //hand (out) ---->---- finger (in)
    private Map<String, EdgeLabel> inEdgeLabels = new HashMap<>();
    private Map<String, EdgeLabel> outEdgeLabels = new HashMap<>();
    private Map<String, EdgeLabel> uncommittedInEdgeLabels = new HashMap<>();
    private Map<String, EdgeLabel> uncommittedOutEdgeLabels = new HashMap<>();

    public static VertexLabel createVertexLabel(SqlgGraph sqlgGraph, Schema schema, String label, Map<String, PropertyType> columns) {
        VertexLabel vertexLabel = new VertexLabel(schema, label, columns);
        if (!schema.getName().equals(SQLG_SCHEMA)) {
            vertexLabel.createVertexLabelOnDb(sqlgGraph, columns);
            TopologyManager.addVertexLabel(sqlgGraph, schema.getName(), label, columns);
        }
        return vertexLabel;
    }

    VertexLabel(Schema schema, String label, Map<String, PropertyType> columns) {
        super(schema, label, columns);
    }

    public void addToInEdgeLabels(EdgeLabel edgeLabel) {
        this.uncommittedInEdgeLabels.put(edgeLabel.getLabel(), edgeLabel);
    }

    public void addToOutEdgeLabels(EdgeLabel edgeLabel) {
        this.uncommittedOutEdgeLabels.put(edgeLabel.getLabel(), edgeLabel);
    }

    public EdgeLabel addEdgeLabel(SqlgGraph sqlgGraph, String edgeLabelName, VertexLabel inVertexLabel, Map<String, PropertyType> properties) {
        EdgeLabel edgeLabel = EdgeLabel.createEdgeLabel(sqlgGraph, edgeLabelName, this, inVertexLabel, properties);
        if (this.schema.isSqlgSchema()) {
            this.outEdgeLabels.put(edgeLabelName, edgeLabel);
            inVertexLabel.inEdgeLabels.put(edgeLabelName, edgeLabel);
        } else {
            this.uncommittedOutEdgeLabels.put(edgeLabelName, edgeLabel);
            inVertexLabel.uncommittedInEdgeLabels.put(edgeLabelName, edgeLabel);
        }
        return edgeLabel;
    }

    public void ensureColumnsExist(SqlgGraph sqlgGraph, Map<String, PropertyType> columns) {
        Preconditions.checkState(!this.schema.getName().equals(SQLG_SCHEMA), "schema may not be %s", SQLG_SCHEMA);
        for (Map.Entry<String, PropertyType> column : columns.entrySet()) {
            if (!this.properties.containsKey(column.getKey())) {
                if (!this.uncommittedProperties.containsKey(column.getKey())) {
                    this.schema.getTopology().lock();
                    if (!this.uncommittedProperties.containsKey(column.getKey())) {
                        TopologyManager.addVertexColumn(sqlgGraph, this.schema.getName(), VERTEX_PREFIX + getLabel(), column);
                        addColumn(sqlgGraph, this.schema.getName(), VERTEX_PREFIX + getLabel(), ImmutablePair.of(column.getKey(), column.getValue()));
                        this.uncommittedProperties.put(column.getKey(), new Property(column.getKey(), column.getValue()));
                    }
                }
            }
        }
    }


    //TODO refactor out columns as its already in the object as this.properties.
    private void createVertexLabelOnDb(SqlgGraph sqlgGraph, Map<String, PropertyType> columns) {
        StringBuilder sql = new StringBuilder(sqlgGraph.getSqlDialect().createTableStatement());
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema.getName()));
        sql.append(".");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(VERTEX_PREFIX + getLabel()));
        sql.append(" (");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
        sql.append(" ");
        sql.append(sqlgGraph.getSqlDialect().getAutoIncrementPrimaryKeyConstruct());
        if (columns.size() > 0) {
            sql.append(", ");
        }
        buildColumns(sqlgGraph, columns, sql);
        sql.append(")");
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


    public Pair<Set<SchemaTable>, Set<SchemaTable>> getTableLabels() {
        Set<SchemaTable> inSchemaTables = new HashSet<>();
        Set<SchemaTable> outSchemaTables = new HashSet<>();
        for (EdgeLabel inEdgeLabel: inEdgeLabels.values()) {
            inSchemaTables.add(SchemaTable.of(inEdgeLabel.getSchema().getName(), EDGE_PREFIX + inEdgeLabel.getLabel()));
        }
        for (EdgeLabel outEdgeLabel: outEdgeLabels.values()) {
            outSchemaTables.add(SchemaTable.of(outEdgeLabel.getSchema().getName(), EDGE_PREFIX + outEdgeLabel.getLabel()));
        }
        for (EdgeLabel inEdgeLabel: uncommittedInEdgeLabels.values()) {
            inSchemaTables.add(SchemaTable.of(inEdgeLabel.getSchema().getName(), EDGE_PREFIX + inEdgeLabel.getLabel()));
        }
        for (EdgeLabel outEdgeLabel: uncommittedOutEdgeLabels.values()) {
            outSchemaTables.add(SchemaTable.of(outEdgeLabel.getSchema().getName(), EDGE_PREFIX + outEdgeLabel.getLabel()));
        }
        return Pair.of(inSchemaTables, outSchemaTables);
    }

    public void afterCommit() {
        for (Iterator<Map.Entry<String, EdgeLabel>> it = this.uncommittedInEdgeLabels.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, EdgeLabel> entry = it.next();
            this.inEdgeLabels.put(entry.getKey(), entry.getValue());
            it.remove();
        }
        for (Iterator<Map.Entry<String, EdgeLabel>> it = this.uncommittedInEdgeLabels.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, EdgeLabel> entry = it.next();
            this.outEdgeLabels.put(entry.getKey(), entry.getValue());
            it.remove();
        }
        for (Iterator<Map.Entry<String, Property>> it = this.uncommittedProperties.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Property> entry = it.next();
            this.properties.put(entry.getKey(), entry.getValue());
            entry.getValue().afterCommit();
            it.remove();
        }
    }

    public void afterRollback() {
        this.uncommittedOutEdgeLabels.clear();
        this.uncommittedInEdgeLabels.clear();
        for (Iterator<Map.Entry<String, Property>> it = this.uncommittedProperties.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Property> entry = it.next();
            entry.getValue().afterRollback();
            it.remove();
        }
    }

    @Override
    public String toString() {
        String result = "label: " + this.schema.getName() + "." + this.getLabel() + "\n\tin labels:";
        for (EdgeLabel edgeLabel : inEdgeLabels.values()) {
            result += "\n\t" + edgeLabel.getLabel();
        }
        result += "\n\tout labels";
        for (EdgeLabel edgeLabel : outEdgeLabels.values()) {
            result += "\n\t" + edgeLabel.getLabel();
        }
        result += "\n\tuncommitted in labels";
        for (EdgeLabel edgeLabel : uncommittedInEdgeLabels.values()) {
            result += "\n\t" + edgeLabel.getLabel();
        }
        result += "\n\tuncommitted out lables";
        for (EdgeLabel edgeLabel : uncommittedOutEdgeLabels.values()) {
            result += "\n\t" + edgeLabel.getLabel();
        }
        return result;
    }

}
