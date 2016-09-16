package org.umlg.sqlg.topology;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static org.umlg.sqlg.structure.SchemaManager.EDGE_PREFIX;
import static org.umlg.sqlg.structure.SchemaManager.SQLG_SCHEMA;
import static org.umlg.sqlg.structure.SchemaManager.VERTEX_PREFIX;

/**
 * Date: 2016/09/04
 * Time: 8:49 AM
 */
public class VertexLabel extends AbstractElement {

    private Logger logger = LoggerFactory.getLogger(VertexLabel.class.getName());
    private Schema schema;
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

    public Schema getSchema() {
        return schema;
    }

    private VertexLabel(Schema schema, String label, Map<String, PropertyType> columns) {
        super(label, columns);
        this.schema = schema;
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

    public Map<String, PropertyType> getPropertyTypeMap() {

        Map<String, PropertyType> result = new HashMap<>();
        for (Map.Entry<String, Property> propertyEntry : this.properties.entrySet()) {

            result.put(propertyEntry.getValue().getName(), propertyEntry.getValue().getPropertyType());
        }

        return result;
    }

    public Pair<Set<SchemaTable>, Set<SchemaTable>> getTableLabels() {

        Set<SchemaTable> inSchemaTables = new HashSet<>();
        Set<SchemaTable> outSchemaTables = new HashSet<>();

        for (Map.Entry<String, EdgeLabel> inEdgeLabelEntry : inEdgeLabels.entrySet()) {

            inSchemaTables.add(SchemaTable.of(this.schema.getName(), EDGE_PREFIX + inEdgeLabelEntry.getValue().getLabel()));

        }
        for (Map.Entry<String, EdgeLabel> outEdgeLabelEntry : outEdgeLabels.entrySet()) {

            outSchemaTables.add(SchemaTable.of(this.schema.getName(), EDGE_PREFIX + outEdgeLabelEntry.getValue().getLabel()));

        }
        for (Map.Entry<String, EdgeLabel> inEdgeLabelEntry : uncommittedInEdgeLabels.entrySet()) {

            inSchemaTables.add(SchemaTable.of(this.schema.getName(), EDGE_PREFIX + inEdgeLabelEntry.getValue().getLabel()));

        }
        for (Map.Entry<String, EdgeLabel> outEdgeLabelEntry : uncommittedOutEdgeLabels.entrySet()) {

            outSchemaTables.add(SchemaTable.of(this.schema.getName(), EDGE_PREFIX + outEdgeLabelEntry.getValue().getLabel()));

        }
        return Pair.of(inSchemaTables, outSchemaTables);
    }

    public void afterCommit() {

        for (Iterator<Map.Entry<String, EdgeLabel>> it = this.uncommittedInEdgeLabels.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, EdgeLabel> entry = it.next();
            this.inEdgeLabels.put(entry.getKey(), entry.getValue());
            entry.getValue().afterCommit();
            it.remove();
        }

        for (Iterator<Map.Entry<String, EdgeLabel>> it = this.uncommittedInEdgeLabels.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, EdgeLabel> entry = it.next();
            this.outEdgeLabels.put(entry.getKey(), entry.getValue());
            entry.getValue().afterCommit();
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

        for (Iterator<Map.Entry<String, EdgeLabel>> it = this.uncommittedInEdgeLabels.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, EdgeLabel> entry = it.next();
            entry.getValue().afterRollback();
            it.remove();
        }

        for (Iterator<Map.Entry<String, EdgeLabel>> it = this.uncommittedInEdgeLabels.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, EdgeLabel> entry = it.next();
            entry.getValue().afterRollback();
            it.remove();
        }

        for (Iterator<Map.Entry<String, Property>> it = this.uncommittedProperties.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Property> entry = it.next();
            entry.getValue().afterRollback();
            it.remove();
        }

    }

    @Override
    public String toString() {
        String result = this.schema.getName() + ":" + this.getLabel() + "\n\tin labels:";
        for (EdgeLabel edgeLabel : inEdgeLabels.values()) {
            result += "\n\t" + edgeLabel.getLabel();
        }
        result += "\n\tout lables";
        for (EdgeLabel edgeLabel : outEdgeLabels.values()) {
            result += "\n\t" + edgeLabel.getLabel();
        }
        result += "\n\tuncommitted in lables";
        for (EdgeLabel edgeLabel : uncommittedInEdgeLabels.values()) {
            result += "\n\t" + edgeLabel.getLabel();
        }
        result += "\n\tuncommitted out lables";
        for (EdgeLabel edgeLabel : uncommittedOutEdgeLabels.values()) {
            result += "\n\t" + edgeLabel.getLabel();
        }
        return result;
    }

    //TODO this is no good. Need a schema -> edge meta edge
    //As many different VertexLabels can have the same EdgeLabel this will do the same EdgeLabel many times.
    public Map<String, Set<String>> getAllEdgeForeignKeys() {
        Map<String, Set<String>> result = new HashMap<>();
        for (EdgeLabel edgeLabel : inEdgeLabels.values()) {
            result.put(this.schema.getName() + "." + EDGE_PREFIX + edgeLabel.getLabel(), edgeLabel.getAllEdgeForeignKeys());
        }
        for (EdgeLabel edgeLabel : outEdgeLabels.values()) {
            result.put(this.schema.getName() + "." + EDGE_PREFIX + edgeLabel.getLabel(), edgeLabel.getAllEdgeForeignKeys());
        }
        for (EdgeLabel edgeLabel : uncommittedInEdgeLabels.values()) {
            result.put(this.schema.getName() + "." + EDGE_PREFIX + edgeLabel.getLabel(), edgeLabel.getAllEdgeForeignKeys());
        }
        for (EdgeLabel edgeLabel : uncommittedOutEdgeLabels.values()) {
            result.put(this.schema.getName() + "." + EDGE_PREFIX + edgeLabel.getLabel(), edgeLabel.getAllEdgeForeignKeys());
        }
        return result;
    }
}
