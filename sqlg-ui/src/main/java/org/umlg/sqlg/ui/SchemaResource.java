package org.umlg.sqlg.ui;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.tuple.Pair;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.ui.util.ISlickLazyTree;
import org.umlg.sqlg.ui.util.ObjectMapperFactory;
import org.umlg.sqlg.ui.util.SlickLazyTree;
import org.umlg.sqlg.ui.util.SlickLazyTreeContainer;
import spark.Request;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SchemaResource {

    public static ArrayNode retrieveSchemas(Request req) {
        Pair<ListOrderedSet<SlickLazyTree>, ListOrderedSet<SlickLazyTree>> rootNodes = retrieveRootNodes();
        ListOrderedSet<SlickLazyTree> roots = rootNodes.getRight();
        ListOrderedSet<SlickLazyTree> initialEntries = rootNodes.getLeft();
        SlickLazyTreeContainer treeContainer = new SlickLazyTreeContainer(roots);
        @SuppressWarnings("UnnecessaryLocalVariable")
        ArrayNode result = treeContainer.complete(new SchemaTreeSlickLazyTreeHelper(initialEntries));
        return result;
    }

    private static Pair<ListOrderedSet<SlickLazyTree>, ListOrderedSet<SlickLazyTree>> retrieveRootNodes() {
        ListOrderedSet<SlickLazyTree> roots = new ListOrderedSet<>();
        ListOrderedSet<SlickLazyTree> allEntries = new ListOrderedSet<>();
        ObjectMapper objectMapper = ObjectMapperFactory.INSTANCE.getObjectMapper();

        SqlgGraph sqlgGraph = SqlgUI.INSTANCE.getSqlgGraph();
        Set<Schema> schemas = sqlgGraph.getTopology().getSchemas();

        for (Schema schema : schemas) {
            ObjectNode schemaRow = createSchemaTreeItem(objectMapper, schema);
            SlickLazyTree vendorTechnologyGroupSlickLazyTree = SlickLazyTree.from(schemaRow);
            roots.add(vendorTechnologyGroupSlickLazyTree);
            allEntries.add(vendorTechnologyGroupSlickLazyTree);
            ArrayNode networkNodeChildrenArrayNode = (ArrayNode) schemaRow.get("children");

            Collection<VertexLabel> vertexLabels = schema.getVertexLabels().values();
            for (VertexLabel vertexLabel: vertexLabels) {
                ObjectNode vertexLabelRow = createVertexLabelTreeItem(objectMapper, schema, vertexLabel);
                networkNodeChildrenArrayNode.add(vertexLabelRow.get("id"));
                SlickLazyTree networkNodeSlickLazyTree = SlickLazyTree.from(vertexLabelRow);
                allEntries.add(networkNodeSlickLazyTree);
                networkNodeSlickLazyTree.setParent(vendorTechnologyGroupSlickLazyTree);
                networkNodeSlickLazyTree.setChildrenIsLoaded(true);
                vendorTechnologyGroupSlickLazyTree.addChild(networkNodeSlickLazyTree);
            }
        }

        return Pair.of(allEntries, roots);
    }

    private static ObjectNode createSchemaTreeItem(ObjectMapper objectMapper, Schema schema) {
        ObjectNode vendorTechnologyGroupRow = objectMapper.createObjectNode();
        vendorTechnologyGroupRow.put("id", schema.getName());
        vendorTechnologyGroupRow.put("title", schema.getName());
        vendorTechnologyGroupRow.put("icon", "fas fa-tag");
        vendorTechnologyGroupRow.put("value", schema.getName());
        vendorTechnologyGroupRow.put("indent", 0);
        vendorTechnologyGroupRow.put("_collapsed", true);
        vendorTechnologyGroupRow.put("isLeaf", false);
        vendorTechnologyGroupRow.putNull("parent");
        vendorTechnologyGroupRow.set("parents", objectMapper.createArrayNode());
        vendorTechnologyGroupRow.set("children", objectMapper.createArrayNode());
        vendorTechnologyGroupRow.put("checkBox", false);
        vendorTechnologyGroupRow.put("selected", false);
        vendorTechnologyGroupRow.put("partakesInSelectionFilter", false);
        vendorTechnologyGroupRow.put("selectAllChildrenCheckBox", false);
        vendorTechnologyGroupRow.put("_fetched", true);
        vendorTechnologyGroupRow.put("type", Schema.class.getSimpleName());
        return vendorTechnologyGroupRow;
    }

    private static ObjectNode createVertexLabelTreeItem(ObjectMapper objectMapper, Schema schema, VertexLabel vertexLabel) {
        ObjectNode networkNodeRow = objectMapper.createObjectNode();
        networkNodeRow.put("id", schema.getName() + "_" + vertexLabel.getName());
        networkNodeRow.put("title", vertexLabel.getName());
        networkNodeRow.put("icon", "database");
        networkNodeRow.put("value", vertexLabel.getName());
        networkNodeRow.put("indent", 2);
        networkNodeRow.put("_collapsed", true);
        networkNodeRow.put("isLeaf", true);
        networkNodeRow.put("parent", schema.getName());
        ArrayNode parents = objectMapper.createArrayNode();
        parents.add(schema.getName());
        networkNodeRow.set("parents", parents);
        networkNodeRow.set("children", objectMapper.createArrayNode());
        networkNodeRow.put("checkBox", false);
        networkNodeRow.put("selected", false);
        networkNodeRow.put("partakesInSelectionFilter", false);
        networkNodeRow.put("selectAllChildrenCheckBox", false);
        networkNodeRow.put("_fetched", true);
        networkNodeRow.put("type", "NetworkNode");
        return networkNodeRow;
    }

    public static class SchemaTreeSlickLazyTreeHelper implements ISlickLazyTree {

        private Map<String, SlickLazyTree> initialEntryMap = new HashMap<>();

        public SchemaTreeSlickLazyTreeHelper(ListOrderedSet<SlickLazyTree> initialEntries) {
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
