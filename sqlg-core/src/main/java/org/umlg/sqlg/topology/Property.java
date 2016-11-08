package org.umlg.sqlg.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.TopologyManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.umlg.sqlg.structure.SchemaManager.EDGE_PREFIX;
import static org.umlg.sqlg.structure.SchemaManager.VERTEX_PREFIX;

/**
 * Date: 2016/09/04
 * Time: 8:50 AM
 */
public class Property {

    private Logger logger = LoggerFactory.getLogger(Property.class.getName());
    private AbstractElement abstractElement;
    private String name;
    private PropertyType propertyType;
    private Index index;
    private Index uncommittedIndex;

    public Property(AbstractElement abstractElement, String name, PropertyType propertyType) {
        this(abstractElement, name, propertyType, Index.NONE);
    }

    public Property(AbstractElement abstractElement, String name, PropertyType propertyType, Index index) {
        this.abstractElement = abstractElement;
        this.name = name;
        this.propertyType = propertyType;
        this.index = index;
    }

    public String getName() {
        return name;
    }

    public PropertyType getPropertyType() {
        return propertyType;
    }

    public Index getIndex() {
        return index;
    }

    public void ensureIndexExist(SqlgGraph sqlgGraph, Index index) {
        sqlgGraph.tx().readWrite();
        if (this.index == Index.NONE) {
            if (this.uncommittedIndex == null) {
                this.abstractElement.getSchema().getTopology().lock();
                if (this.uncommittedIndex == null) {
                    Schema schema = this.abstractElement.getSchema();
                    TopologyManager.addPropertyIndex(
                            sqlgGraph,
                            schema.getName(),
                            (this.abstractElement instanceof VertexLabel ? VERTEX_PREFIX : EDGE_PREFIX) + this.abstractElement.getLabel(),
                            Pair.of(this.name, this.propertyType),
                            index
                    );
                    addIndex(
                            sqlgGraph,
                            SchemaTable.of(
                                    schema.getName(),
                                    this.abstractElement.getLabel()
                            ),
                            Pair.of(this.name, this.propertyType),
                            index
                    );
                    this.uncommittedIndex = index;
                }
            }
        }
    }

    private void addIndex(SqlgGraph sqlgGraph, SchemaTable schemaTable, Pair<String, PropertyType> namePropertyTypePair, Index index) {
        String prefix = this.abstractElement instanceof VertexLabel ? VERTEX_PREFIX : EDGE_PREFIX;
        StringBuilder sql = new StringBuilder("CREATE ");
        if (index == Index.UNIQUE) {
            sql.append("UNIQUE ");
        }
        sql.append("INDEX");
        SqlDialect sqlDialect = sqlgGraph.getSqlDialect();
        sql.append(sqlDialect.maybeWrapInQoutes(sqlDialect.indexName(schemaTable, prefix, namePropertyTypePair.getKey())));
        sql.append(" ON ");
        sql.append(sqlDialect.maybeWrapInQoutes(schemaTable.getSchema()));
        sql.append(".");
        sql.append(sqlDialect.maybeWrapInQoutes(prefix + schemaTable.getTable()));
        sql.append(" (");
        sql.append(sqlDialect.maybeWrapInQoutes(namePropertyTypePair.getKey()));
        sql.append(")");
        if (sqlDialect.needsSemicolon()) {
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

    void afterCommit() {
        if (this.uncommittedIndex != null) {
            this.index = this.uncommittedIndex;
        }
        this.uncommittedIndex = null;
    }

    void afterRollback() {
        this.uncommittedIndex = null;
    }

    public JsonNode toNotifyJson() {
        ObjectNode propertyObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        propertyObjectNode.put("name", this.name);
        propertyObjectNode.put("propertyType", this.propertyType.name());
        return propertyObjectNode;
    }

    public static Property fromNotifyJson(AbstractElement abstractElement, JsonNode jsonNode) {
        Property property = new Property(
                abstractElement,
                jsonNode.get("name").asText(),
                PropertyType.valueOf(jsonNode.get("propertyType").asText()));
        return property;
    }

    @Override
    public int hashCode() {
        return this.getName().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof Property)) {
            return false;
        }
        Property other = (Property)o;
        return this.getName().equals(other.getName())  &&
                this.getPropertyType() == other.getPropertyType() &&
                this.getIndex() == other.getIndex();
    }
}
