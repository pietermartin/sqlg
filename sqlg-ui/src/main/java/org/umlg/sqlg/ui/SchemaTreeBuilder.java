package org.umlg.sqlg.ui;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.tuple.Pair;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.PropertyColumn;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.ui.util.ISlickLazyTree;
import org.umlg.sqlg.ui.util.ObjectMapperFactory;
import org.umlg.sqlg.ui.util.SlickLazyTree;

import java.util.*;

public class SchemaTreeBuilder {

    static Pair<ListOrderedSet<SlickLazyTree>, ListOrderedSet<SlickLazyTree>> retrieveRootNodes(String selectedItemId) {
        ListOrderedSet<SlickLazyTree> roots = new ListOrderedSet<>();
        ListOrderedSet<SlickLazyTree> allEntries = new ListOrderedSet<>();
        ObjectMapper objectMapper = ObjectMapperFactory.INSTANCE.getObjectMapper();

        ObjectNode metaSchemaRow = createMetaSchemaTreeItem(objectMapper);
        ArrayNode metaSchemaChildrenArrayNode = (ArrayNode) metaSchemaRow.get("children");
        SlickLazyTree metaSchemaSlickLazyTree = SlickLazyTree.from(metaSchemaRow);
        roots.add(metaSchemaSlickLazyTree);
        allEntries.add(metaSchemaSlickLazyTree);

        SqlgGraph sqlgGraph = SqlgUI.INSTANCE.getSqlgGraph();
        Set<Schema> schemas = sqlgGraph.getTopology().getSchemas();
        for (Schema schema : schemas) {
            String metaSchemaId = metaSchemaRow.get("id").asText();
            ObjectNode schemaRow = createSchemaTreeItem(objectMapper, metaSchemaId, schema);
            metaSchemaChildrenArrayNode.add(schemaRow.get("id"));
            SlickLazyTree schemaSlickLazyTree = SlickLazyTree.from(schemaRow);
            schemaSlickLazyTree.setParent(metaSchemaSlickLazyTree);
            schemaSlickLazyTree.setChildrenIsLoaded(true);
            metaSchemaSlickLazyTree.addChild(schemaSlickLazyTree);
            allEntries.add(schemaSlickLazyTree);
            ArrayNode metaVertexEdgeChildrenArrayNode = (ArrayNode) schemaRow.get("children");
            if (selectedItemId.equals(schemaRow.get("id").asText())) {
                metaSchemaRow.put("_collapsed", false);
                schemaRow.put("_collapsed", true);
                schemaRow.put("selected", true);
            }
            List<String> metaVertexEdges = List.of("vertex", "edge");
            for (String metaVertexEdge : metaVertexEdges) {
                ObjectNode metaVertexEdgeRow = createMetaVertexEdgeTreeItem(objectMapper, metaSchemaId, schema, metaVertexEdge);
                metaVertexEdgeChildrenArrayNode.add(metaVertexEdgeRow.get("id"));
                SlickLazyTree metaVertexEdgeSlickLazyTree = SlickLazyTree.from(metaVertexEdgeRow);
                metaVertexEdgeSlickLazyTree.setParent(schemaSlickLazyTree);
                metaVertexEdgeSlickLazyTree.setChildrenIsLoaded(true);
                schemaSlickLazyTree.addChild(metaVertexEdgeSlickLazyTree);
                allEntries.add(schemaSlickLazyTree);
                ArrayNode vertexEdgeChildrenArrayNode = (ArrayNode) metaVertexEdgeRow.get("children");
                if (selectedItemId.equals(metaVertexEdgeRow.get("id").asText())) {
                    metaSchemaRow.put("_collapsed", false);
                    schemaRow.put("_collapsed", false);
                    metaVertexEdgeRow.put("_collapsed", true);
                    metaVertexEdgeRow.put("selected", true);
                }
                if (metaVertexEdge.equals("vertex")) {
                    Collection<VertexLabel> vertexLabels = schema.getVertexLabels().values();
                    for (VertexLabel vertexLabel : vertexLabels) {
                        ObjectNode vertexLabelRow = createVertexLabelTreeItem(objectMapper, metaSchemaId, schema, metaVertexEdge, vertexLabel);
                        vertexEdgeChildrenArrayNode.add(vertexLabelRow.get("id"));
                        SlickLazyTree vertexLabelSlickLazyTree = SlickLazyTree.from(vertexLabelRow);
                        allEntries.add(vertexLabelSlickLazyTree);
                        vertexLabelSlickLazyTree.setParent(metaVertexEdgeSlickLazyTree);
                        vertexLabelSlickLazyTree.setChildrenIsLoaded(true);
                        metaVertexEdgeSlickLazyTree.addChild(vertexLabelSlickLazyTree);
                        ArrayNode propertyColumnChildrenArrayNode = (ArrayNode) vertexLabelRow.get("children");
                        if (selectedItemId.equals(vertexLabelRow.get("id").asText())) {
                            metaSchemaRow.put("_collapsed", false);
                            schemaRow.put("_collapsed", false);
                            metaVertexEdgeRow.put("_collapsed", false);
                            vertexLabelRow.put("_collapsed", true);
                            vertexLabelRow.put("selected", true);
                        }
                    }
                } else {
                    Collection<EdgeLabel> edgeLabels = schema.getEdgeLabels().values();
                    for (EdgeLabel edgeLabel : edgeLabels) {
                        ObjectNode edgeLabelRow = createEdgeLabelTreeItem(objectMapper, metaSchemaId, schema, metaVertexEdge, edgeLabel);
                        vertexEdgeChildrenArrayNode.add(edgeLabelRow.get("id"));
                        SlickLazyTree edgeLabelSlickLazyTree = SlickLazyTree.from(edgeLabelRow);
                        allEntries.add(edgeLabelSlickLazyTree);
                        edgeLabelSlickLazyTree.setParent(metaVertexEdgeSlickLazyTree);
                        edgeLabelSlickLazyTree.setChildrenIsLoaded(true);
                        metaVertexEdgeSlickLazyTree.addChild(edgeLabelSlickLazyTree);
                        ArrayNode propertyColumnChildrenArrayNode = (ArrayNode) edgeLabelRow.get("children");
                        if (selectedItemId.equals(edgeLabelRow.get("id").asText())) {
                            metaSchemaRow.put("_collapsed", false);
                            schemaRow.put("_collapsed", false);
                            metaVertexEdgeRow.put("_collapsed", false);
                            edgeLabelRow.put("_collapsed", true);
                            edgeLabelRow.put("selected", true);
                        }
                    }
                }
            }
        }
        return Pair.of(allEntries, roots);
    }

    private static ObjectNode createMetaSchemaTreeItem(ObjectMapper objectMapper) {
        ObjectNode schemaRow = objectMapper.createObjectNode();
        schemaRow.put("id", "metaSchema");
        schemaRow.put("title", "Schemas");
        schemaRow.put("icon", "fas fa-database");
        schemaRow.put("value", "metaSchema");
        schemaRow.put("indent", 0);
        schemaRow.put("_collapsed", true);
        schemaRow.put("isLeaf", false);
        schemaRow.putNull("parent");
        schemaRow.set("parents", objectMapper.createArrayNode());
        schemaRow.set("children", objectMapper.createArrayNode());
        schemaRow.put("checkBox", false);
        schemaRow.put("selected", false);
        schemaRow.put("partakesInSelectionFilter", false);
        schemaRow.put("selectAllChildrenCheckBox", false);
        schemaRow.put("_fetched", true);
        schemaRow.put("type", "MetaSchema");
        return schemaRow;
    }

    private static ObjectNode createSchemaTreeItem(ObjectMapper objectMapper, String parent, Schema schema) {
        ObjectNode schemaRow = objectMapper.createObjectNode();
        schemaRow.put("id", schema.getName());
        schemaRow.put("title", schema.getName());
        schemaRow.put("icon", "fas fa-tag");
        schemaRow.put("value", schema.getName());
        schemaRow.put("schemaName", schema.getName());
        schemaRow.put("indent", 1);
        schemaRow.put("_collapsed", true);
        schemaRow.put("isLeaf", false);
        schemaRow.put("parent", parent);
        ArrayNode parents = objectMapper.createArrayNode();
        parents.add(parent);
        schemaRow.set("parents", parents);
        schemaRow.set("children", objectMapper.createArrayNode());
        schemaRow.put("checkBox", false);
        schemaRow.put("selected", false);
        schemaRow.put("partakesInSelectionFilter", false);
        schemaRow.put("selectAllChildrenCheckBox", false);
        schemaRow.put("_fetched", true);
        schemaRow.put("type", Schema.class.getSimpleName());
        return schemaRow;
    }

    private static ObjectNode createMetaVertexEdgeTreeItem(ObjectMapper objectMapper, String metaSchemaId, Schema schema, String metaVertexEdge) {
        ObjectNode schemaRow = objectMapper.createObjectNode();
        schemaRow.put("id", metaSchemaId + "_" + schema.getName() + "_" + metaVertexEdge);
        schemaRow.put("title", (metaVertexEdge.equals("vertex") ? "Vertex labels" : "Edge labels"));
        schemaRow.put("icon", "fas " + (metaVertexEdge.equals("vertex") ? "fa-dot-circle" : "fa-arrows-alt-h"));
        schemaRow.put("value", metaVertexEdge);
        schemaRow.put("indent", 2);
        schemaRow.put("_collapsed", true);
        schemaRow.put("isLeaf", false);
        schemaRow.put("parent", metaSchemaId + "_" + schema.getName());
        ArrayNode parents = objectMapper.createArrayNode();
        parents.add(metaSchemaId);
        parents.add(schema.getName());
        schemaRow.set("parents", parents);
        schemaRow.set("children", objectMapper.createArrayNode());
        schemaRow.put("checkBox", false);
        schemaRow.put("selected", false);
        schemaRow.put("partakesInSelectionFilter", false);
        schemaRow.put("selectAllChildrenCheckBox", false);
        schemaRow.put("_fetched", true);
        schemaRow.put("type", "MetaVertexEdge");
        return schemaRow;
    }

    private static ObjectNode createVertexLabelTreeItem(ObjectMapper objectMapper, String metaSchemaId, Schema schema, String metaVertexEdge, VertexLabel vertexLabel) {
        ObjectNode vertexLabelRow = objectMapper.createObjectNode();
        vertexLabelRow.put("id", metaSchemaId + "_" + schema.getName() + "_" + metaVertexEdge + "_" + vertexLabel.getName());
        vertexLabelRow.put("title", vertexLabel.getName());
        vertexLabelRow.put("icon", "fas fa-table");
        vertexLabelRow.put("value", vertexLabel.getName());
        vertexLabelRow.put("schemaName", schema.getName());
        vertexLabelRow.put("abstractLabel", vertexLabel.getName());
        vertexLabelRow.put("vertexOrEdge", "vertex");
        vertexLabelRow.put("indent", 3);
        vertexLabelRow.put("_collapsed", true);
        vertexLabelRow.put("isLeaf", true);
        vertexLabelRow.put("parent", metaSchemaId + "_" + schema.getName() + "_" + metaVertexEdge);
        ArrayNode parents = objectMapper.createArrayNode();
        parents.add(metaSchemaId);
        parents.add(schema.getName());
        parents.add(metaVertexEdge);
        vertexLabelRow.set("parents", parents);
        vertexLabelRow.set("children", objectMapper.createArrayNode());
        vertexLabelRow.put("checkBox", false);
        vertexLabelRow.put("selected", false);
        vertexLabelRow.put("partakesInSelectionFilter", false);
        vertexLabelRow.put("selectAllChildrenCheckBox", false);
        vertexLabelRow.put("_fetched", true);
        vertexLabelRow.put("type", VertexLabel.class.getSimpleName());
        return vertexLabelRow;
    }

    private static ObjectNode createEdgeLabelTreeItem(ObjectMapper objectMapper, String metaSchemaId, Schema schema, String metaVertexEdge, EdgeLabel edgeLabel) {
        ObjectNode edgeLabelRow = objectMapper.createObjectNode();
        edgeLabelRow.put("id", metaSchemaId + "_" + schema.getName() + "_" + metaVertexEdge + "_" + edgeLabel.getName());
        edgeLabelRow.put("title", edgeLabel.getName());
        edgeLabelRow.put("icon", "fas fa-table");
        edgeLabelRow.put("value", edgeLabel.getName());
        edgeLabelRow.put("schemaName", schema.getName());
        edgeLabelRow.put("abstractLabel", edgeLabel.getName());
        edgeLabelRow.put("vertexOrEdge", "edge");
        edgeLabelRow.put("indent", 3);
        edgeLabelRow.put("_collapsed", true);
        edgeLabelRow.put("isLeaf", true);
        edgeLabelRow.put("parent", metaSchemaId + "_" + schema.getName() + "_" + metaVertexEdge);
        ArrayNode parents = objectMapper.createArrayNode();
        parents.add(metaSchemaId);
        parents.add(schema.getName());
        parents.add(metaVertexEdge);
        edgeLabelRow.set("parents", parents);
        edgeLabelRow.set("children", objectMapper.createArrayNode());
        edgeLabelRow.put("checkBox", false);
        edgeLabelRow.put("selected", false);
        edgeLabelRow.put("partakesInSelectionFilter", false);
        edgeLabelRow.put("selectAllChildrenCheckBox", false);
        edgeLabelRow.put("_fetched", true);
        edgeLabelRow.put("type", EdgeLabel.class.getSimpleName());
        return edgeLabelRow;
    }

    private static ObjectNode createPropertyColumnTreeItem(ObjectMapper objectMapper, Schema schema, VertexLabel vertexLabel, PropertyColumn propertyColumn) {
        ObjectNode propertyColumnRow = objectMapper.createObjectNode();
        propertyColumnRow.put("id", schema.getName() + "_" + vertexLabel.getName() + "_" + propertyColumn.getName());
        propertyColumnRow.put("title", propertyColumn.getName());
        propertyColumnRow.put("icon", "fas fa-columns");
        propertyColumnRow.put("value", propertyColumn.getName());
        propertyColumnRow.put("indent", 3);
        propertyColumnRow.put("_collapsed", true);
        propertyColumnRow.put("isLeaf", true);
        propertyColumnRow.put("parent", schema.getName() + "_" + vertexLabel.getName());
        ArrayNode parents = objectMapper.createArrayNode();
        parents.add(schema.getName());
        propertyColumnRow.set("parents", parents);
        propertyColumnRow.set("children", objectMapper.createArrayNode());
        propertyColumnRow.put("checkBox", false);
        propertyColumnRow.put("selected", false);
        propertyColumnRow.put("partakesInSelectionFilter", false);
        propertyColumnRow.put("selectAllChildrenCheckBox", false);
        propertyColumnRow.put("_fetched", true);
        propertyColumnRow.put("type", "NetworkNode");
        return propertyColumnRow;
    }

    static class SchemaTreeSlickLazyTreeHelper implements ISlickLazyTree {

        private Map<String, SlickLazyTree> initialEntryMap = new HashMap<>();

        SchemaTreeSlickLazyTreeHelper(ListOrderedSet<SlickLazyTree> initialEntries) {
            for (SlickLazyTree initialEntry : initialEntries) {
                this.initialEntryMap.put(initialEntry.getId(), initialEntry);
            }
        }

        @Override
        public SlickLazyTree parent(SlickLazyTree entry) {
            return null;
        }

        @Override
        public ListOrderedSet<SlickLazyTree> children(SlickLazyTree parent) {
            return null;
        }

        @Override
        public void refresh(SlickLazyTree slickLazyTree) {

        }
    }
}
