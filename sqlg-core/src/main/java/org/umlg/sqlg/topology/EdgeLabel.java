package org.umlg.sqlg.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SchemaManager;
import org.umlg.sqlg.structure.SqlgGraph;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.umlg.sqlg.structure.SchemaManager.EDGE_PREFIX;
import static org.umlg.sqlg.structure.SchemaManager.SQLG_SCHEMA;
import static org.umlg.sqlg.structure.SchemaManager.VERTEX_PREFIX;

/**
 * Date: 2016/09/04
 * Time: 8:49 AM
 */
public class EdgeLabel extends AbstractElement {

    private Logger logger = LoggerFactory.getLogger(EdgeLabel.class.getName());
    //This just won't stick in my brain.
    //hand (out) ---->---- finger (in)
    private Set<VertexLabel> outVertexLabels = new HashSet<>();
    private Set<VertexLabel> inVertexLabels = new HashSet<>();
    private Set<VertexLabel> uncommittedOutVertexLabels = new HashSet<>();
    private Set<VertexLabel> uncommittedInVertexLabels = new HashSet<>();

    public static EdgeLabel createEdgeLabel(SqlgGraph sqlgGraph, String edgeLabelName, VertexLabel outVertexLabel, VertexLabel inVertexLabel, Map<String, PropertyType> properties) {

        EdgeLabel edgeLabel = new EdgeLabel(edgeLabelName, inVertexLabel, outVertexLabel, properties);
        if (!inVertexLabel.getSchema().getName().equals(SQLG_SCHEMA)) {
            edgeLabel.createEdgeTable(sqlgGraph, inVertexLabel, outVertexLabel, properties);
        }
        return edgeLabel;

    }

    private EdgeLabel(String edgeLabelName, VertexLabel inVertexLabel, VertexLabel outVertexLabel, Map<String, PropertyType> properties) {
        super(edgeLabelName, properties);
        this.uncommittedInVertexLabels.add(inVertexLabel);
        this.uncommittedOutVertexLabels.add(outVertexLabel);
    }

    private void createEdgeTable(SqlgGraph sqlgGraph, VertexLabel inVertexLabel, VertexLabel outVertexLabel, Map<String, PropertyType> columns) {

        String schema = inVertexLabel.getSchema().getName();
        String tableName = EDGE_PREFIX + getLabel();

        SqlDialect sqlDialect = sqlgGraph.getSqlDialect();
        sqlDialect.assertTableName(tableName);
        StringBuilder sql = new StringBuilder(sqlDialect.createTableStatement());
        sql.append(sqlDialect.maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(sqlDialect.maybeWrapInQoutes(tableName));
        sql.append("(");
        sql.append(sqlDialect.maybeWrapInQoutes("ID"));
        sql.append(" ");
        sql.append(sqlDialect.getAutoIncrementPrimaryKeyConstruct());
        if (columns.size() > 0) {
            sql.append(", ");
        }
        buildColumns(sqlgGraph, columns, sql);
        sql.append(", ");
//        sql.append(sqlDialect.maybeWrapInQoutes(foreignKeyIn.getSchema() + "." + foreignKeyIn.getTable() + SchemaManager.IN_VERTEX_COLUMN_END));
        sql.append(sqlDialect.maybeWrapInQoutes(inVertexLabel.getSchema().getName() + "." + inVertexLabel.getLabel() + SchemaManager.IN_VERTEX_COLUMN_END));
        sql.append(" ");
        sql.append(sqlDialect.getForeignKeyTypeDefinition());
        sql.append(", ");
//        sql.append(sqlDialect.maybeWrapInQoutes(foreignKeyOut.getSchema() + "." + foreignKeyOut.getTable() + SchemaManager.OUT_VERTEX_COLUMN_END));
        sql.append(sqlDialect.maybeWrapInQoutes(outVertexLabel.getSchema().getName() + "." + outVertexLabel.getLabel() + SchemaManager.OUT_VERTEX_COLUMN_END));
        sql.append(" ");
        sql.append(sqlDialect.getForeignKeyTypeDefinition());

        //foreign key definition start
        if (sqlgGraph.isImplementForeignKeys()) {
            sql.append(", ");
            sql.append("FOREIGN KEY (");
//            sql.append(sqlDialect.maybeWrapInQoutes(foreignKeyIn.getSchema() + "." + foreignKeyIn.getTable() + SchemaManager.IN_VERTEX_COLUMN_END));
            sql.append(sqlDialect.maybeWrapInQoutes(inVertexLabel.getSchema().getName() + "." + inVertexLabel.getLabel() + SchemaManager.IN_VERTEX_COLUMN_END));
            sql.append(") REFERENCES ");
//            sql.append(sqlDialect.maybeWrapInQoutes(foreignKeyIn.getSchema()));
            sql.append(sqlDialect.maybeWrapInQoutes(inVertexLabel.getSchema().getName()));
            sql.append(".");
//            sql.append(sqlDialect.maybeWrapInQoutes(VERTEX_PREFIX + foreignKeyIn.getTable()));
            sql.append(sqlDialect.maybeWrapInQoutes(VERTEX_PREFIX + inVertexLabel.getLabel()));
            sql.append(" (");
            sql.append(sqlDialect.maybeWrapInQoutes("ID"));
            sql.append("), ");
            sql.append(" FOREIGN KEY (");
//            sql.append(sqlDialect.maybeWrapInQoutes(foreignKeyOut.getSchema() + "." + foreignKeyOut.getTable() + SchemaManager.OUT_VERTEX_COLUMN_END));
            sql.append(sqlDialect.maybeWrapInQoutes(outVertexLabel.getSchema().getName() + "." + outVertexLabel.getLabel() + SchemaManager.OUT_VERTEX_COLUMN_END));
            sql.append(") REFERENCES ");
//            sql.append(sqlDialect.maybeWrapInQoutes(foreignKeyOut.getSchema()));
            sql.append(sqlDialect.maybeWrapInQoutes(outVertexLabel.getSchema().getName()));
            sql.append(".");
//            sql.append(sqlDialect.maybeWrapInQoutes(VERTEX_PREFIX + foreignKeyOut.getTable()));
            sql.append(sqlDialect.maybeWrapInQoutes(VERTEX_PREFIX + outVertexLabel.getLabel()));
            sql.append(" (");
            sql.append(sqlDialect.maybeWrapInQoutes("ID"));
            sql.append(")");
        }
        //foreign key definition end

        sql.append(")");
        if (sqlDialect.needsSemicolon()) {
            sql.append(";");
        }

        if (sqlDialect.needForeignKeyIndex()) {
            sql.append("\nCREATE INDEX ON ");
            sql.append(sqlDialect.maybeWrapInQoutes(schema));
            sql.append(".");
            sql.append(sqlDialect.maybeWrapInQoutes(tableName));
            sql.append(" (");
//            sql.append(sqlDialect.maybeWrapInQoutes(foreignKeyIn.getSchema() + "." + foreignKeyIn.getTable() + SchemaManager.IN_VERTEX_COLUMN_END));
            sql.append(sqlDialect.maybeWrapInQoutes(inVertexLabel.getSchema().getName() + "." + inVertexLabel.getLabel() + SchemaManager.IN_VERTEX_COLUMN_END));
            sql.append(");");

            sql.append("\nCREATE INDEX ON ");
            sql.append(sqlDialect.maybeWrapInQoutes(schema));
            sql.append(".");
            sql.append(sqlDialect.maybeWrapInQoutes(tableName));
            sql.append(" (");
//            sql.append(sqlDialect.maybeWrapInQoutes(foreignKeyOut.getSchema() + "." + foreignKeyOut.getTable() + SchemaManager.OUT_VERTEX_COLUMN_END));
            sql.append(sqlDialect.maybeWrapInQoutes(outVertexLabel.getSchema().getName() + "." + outVertexLabel.getLabel() + SchemaManager.OUT_VERTEX_COLUMN_END));
            sql.append(");");
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

    public void afterCommit() {

        for (Iterator<VertexLabel> it = this.uncommittedInVertexLabels.iterator(); it.hasNext(); ) {
            VertexLabel vertexLabel = it.next();
            this.inVertexLabels.add(vertexLabel);
            it.remove();
        }

        for (Iterator<VertexLabel> it = this.uncommittedOutVertexLabels.iterator(); it.hasNext(); ) {
            VertexLabel vertexLabel = it.next();
            this.outVertexLabels.add(vertexLabel);
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

        this.uncommittedInVertexLabels.clear();
        this.uncommittedOutVertexLabels.clear();

        for (Iterator<Map.Entry<String, Property>> it = this.uncommittedProperties.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Property> entry = it.next();
            entry.getValue().afterRollback();
            it.remove();
        }

    }

    @Override
    public String toString() {
        return getLabel() +
                "\n\tin vertex: [" +
                this.inVertexLabels.stream().map(VertexLabel::getLabel).reduce((x, y) -> x + ", " + y).orElse("") +
                "]\n\tout vertex: [" +
                this.outVertexLabels.stream().map(VertexLabel::getLabel).reduce((x, y) -> x + ", " + y).orElse("") +
                "]";
    }

    public Set<String> getAllEdgeForeignKeys() {
        Set<String> result = new HashSet<>();
        for (VertexLabel vertexLabel : this.inVertexLabels) {
            result.add(vertexLabel.getSchema().getName() + "." + vertexLabel.getLabel() + SchemaManager.IN_VERTEX_COLUMN_END);
        }
        for (VertexLabel vertexLabel : this.outVertexLabels) {
            result.add(vertexLabel.getSchema().getName() + "." + vertexLabel.getLabel() + SchemaManager.OUT_VERTEX_COLUMN_END);
        }
        for (VertexLabel vertexLabel : this.uncommittedInVertexLabels) {
            result.add(vertexLabel.getSchema().getName() + "." + vertexLabel.getLabel() + SchemaManager.IN_VERTEX_COLUMN_END);
        }
        for (VertexLabel vertexLabel : this.uncommittedOutVertexLabels) {
            result.add(vertexLabel.getSchema().getName() + "." + vertexLabel.getLabel() + SchemaManager.OUT_VERTEX_COLUMN_END);
        }
        return result;
    }
}
