package org.umlg.sqlg.ui;

import com.auth0.jwt.interfaces.DecodedJWT;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.ui.util.*;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.*;

public class SchemaResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaResource.class);

    public static ObjectNode userAllowedToEdit(Request req, Response res) {
        ObjectMapper mapper = ObjectMapperFactory.INSTANCE.getObjectMapper();
        String token = req.cookie(AuthUtil.SQLG_TOKEN);
        DecodedJWT jwt = AuthUtil.validToken(token);
        Preconditions.checkState(jwt != null);
        String username = jwt.getClaim("username").asString();
        String edit = "sqlg.ui.username." + username + ".edit";
        return mapper.createObjectNode().put("userAllowedToEdit", SqlgUI.INSTANCE.getSqlgGraph().configuration().getBoolean(edit, false));
    }

    public static ObjectNode login(Request req, Response res) {
        ObjectMapper mapper = ObjectMapperFactory.INSTANCE.getObjectMapper();
        ObjectNode response = mapper.createObjectNode();
        String body = req.body();
        ObjectNode requestBody;
        try {
            requestBody = (ObjectNode) mapper.readTree(body);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String username = null;
        String password = null;
        if (requestBody.hasNonNull("username")) {
            username = requestBody.get("username").asText();
        }
        if (requestBody.hasNonNull("password")) {
            password = requestBody.get("password").asText();
        }

        SqlgGraph sqlgGraph = SqlgUI.INSTANCE.getSqlgGraph();
        String passwordInPropertyFile = sqlgGraph.configuration().getString("sqlg.ui.username." + username);
        if (username != null &&
                !StringUtils.isEmpty(passwordInPropertyFile) &&
                passwordInPropertyFile.equals(password)) {

            String token = AuthUtil.generateToken(username);
            res.cookie("/", AuthUtil.SQLG_TOKEN, token, SqlgUI.INSTANCE.getSqlgGraph().configuration().getInt("sqlg.ui.cookie.expiry", 3600), true, false);
            response.put("editable", sqlgGraph.configuration().getBoolean("sqlg.ui.username." + username + ".edit", false));
        } else {
            res.status(HttpStatus.UNAUTHORIZED_401);
            response.put("status", "error");
            response.put("message", "authentication failed");
            res.removeCookie("/", AuthUtil.SQLG_TOKEN);
        }
        return response;
    }

    public static ObjectNode retrieveGraph() {
        ObjectMapper objectMapper = ObjectMapperFactory.INSTANCE.getObjectMapper();
        SqlgGraph sqlgGraph = SqlgUI.INSTANCE.getSqlgGraph();
        Configuration configuration = sqlgGraph.configuration();
        String jdbc = configuration.getString("jdbc.url");
        String username = configuration.getString("jdbc.username");
        return objectMapper.createObjectNode()
                .put("jdbcUrl", jdbc)
                .put("username", username);
    }

    public static ArrayNode retrieveTopologyTree(Request request) {
        String selectedItemId = request.queryParams("selectedItemId");
        Pair<ListOrderedSet<SlickLazyTree>, ListOrderedSet<SlickLazyTree>> rootNodes = SchemaTreeBuilder.retrieveRootNodes(selectedItemId);
        ListOrderedSet<SlickLazyTree> roots = rootNodes.getRight();
        ListOrderedSet<SlickLazyTree> initialEntries = rootNodes.getLeft();
        SlickLazyTreeContainer treeContainer = new SlickLazyTreeContainer(roots);
        @SuppressWarnings("UnnecessaryLocalVariable")
        ArrayNode result = treeContainer.complete(new SchemaTreeBuilder.SchemaTreeSlickLazyTreeHelper(initialEntries));
        return result;
    }

    public static ObjectNode retrieveSchemas(Request req) {
        ObjectMapper objectMapper = ObjectMapperFactory.INSTANCE.getObjectMapper();
        ObjectNode schemasGridData = objectMapper.createObjectNode();
        ArrayNode schemaColumnsColumns = objectMapper.createArrayNode();
        ArrayNode schemaColumnGridData = objectMapper.createArrayNode();
        schemasGridData.set("columns", schemaColumnsColumns);
        schemasGridData.set("data", schemaColumnGridData);
        schemaColumnsColumns.add(new SlickGridColumn.SlickGridColumnBuilder("name", "name", PropertyType.STRING)
                .setMinWidth(220)
                .build().toJson(objectMapper));
        SqlgGraph sqlgGraph = SqlgUI.INSTANCE.getSqlgGraph();
        Set<Schema> schemas = sqlgGraph.getTopology().getSchemas();
        for (Schema schema : schemas) {
            ObjectNode schemaData = objectMapper.createObjectNode();
            schemaData.put("id", schema.getName());
            schemaData.put("name", schema.getName());
            schemaColumnGridData.add(schemaData);
        }
        return schemasGridData;
    }

    public static ObjectNode retrieveVertexLabels(Request req) {
        ObjectMapper objectMapper = ObjectMapperFactory.INSTANCE.getObjectMapper();
        String schemaName = req.params("schemaName");
        ObjectNode vertexLabelsData = objectMapper.createObjectNode();
        ArrayNode vertexLabelColumnsColumns = objectMapper.createArrayNode();
        ArrayNode vertexLabelGridData = objectMapper.createArrayNode();
        vertexLabelsData.set("columns", vertexLabelColumnsColumns);
        vertexLabelsData.set("data", vertexLabelGridData);
        vertexLabelColumnsColumns.add(new SlickGridColumn.SlickGridColumnBuilder("name", "name", PropertyType.STRING)
                .setMinWidth(220)
                .build().toJson(objectMapper));
        SqlgGraph sqlgGraph = SqlgUI.INSTANCE.getSqlgGraph();
        Optional<Schema> schemaOptional = sqlgGraph.getTopology().getSchema(schemaName);
        if (schemaOptional.isPresent()) {
            for (VertexLabel vertexLabel : schemaOptional.get().getVertexLabels().values()) {
                ObjectNode vertexLabelData = objectMapper.createObjectNode();
                vertexLabelData.put("id", vertexLabel.getName());
                vertexLabelData.put("name", vertexLabel.getName());
                vertexLabelGridData.add(vertexLabelData);
            }
        }
        return vertexLabelsData;
    }

    public static ObjectNode retrieveEdgeLabels(Request req) {
        ObjectMapper objectMapper = ObjectMapperFactory.INSTANCE.getObjectMapper();
        String schemaName = req.params("schemaName");
        ObjectNode edgeLabelsData = objectMapper.createObjectNode();
        ArrayNode edgeLabelColumnsColumns = objectMapper.createArrayNode();
        ArrayNode edgeLabelGridData = objectMapper.createArrayNode();
        edgeLabelsData.set("columns", edgeLabelColumnsColumns);
        edgeLabelsData.set("data", edgeLabelGridData);
        edgeLabelColumnsColumns.add(new SlickGridColumn.SlickGridColumnBuilder("name", "name", PropertyType.STRING)
                .setMinWidth(220)
                .build().toJson(objectMapper));
        SqlgGraph sqlgGraph = SqlgUI.INSTANCE.getSqlgGraph();
        Optional<Schema> schemaOptional = sqlgGraph.getTopology().getSchema(schemaName);
        if (schemaOptional.isPresent()) {
            for (EdgeLabel edgeLabel : schemaOptional.get().getEdgeLabels().values()) {
                ObjectNode vertexLabelData = objectMapper.createObjectNode();
                vertexLabelData.put("id", edgeLabel.getName());
                vertexLabelData.put("name", edgeLabel.getName());
                edgeLabelGridData.add(vertexLabelData);
            }
        }
        return edgeLabelsData;
    }

    public static ObjectNode retrieveSchemaDetails(Request req) {
        SqlgGraph sqlgGraph = SqlgUI.INSTANCE.getSqlgGraph();
        String schemaName = req.params("schemaName");
        Optional<Schema> schemaOptional = sqlgGraph.getTopology().getSchema(schemaName);
        if (schemaOptional.isPresent()) {
            Schema schema = schemaOptional.get();
            List<Vertex> schemaVertices = sqlgGraph.topology().V()
                    .hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                    .has(Topology.SQLG_SCHEMA_SCHEMA_NAME, schema.getName())
                    .toList();
            Preconditions.checkState(schemaVertices.size() == 1);
            Vertex schemaVertex = schemaVertices.get(0);
            ObjectMapper objectMapper = ObjectMapperFactory.INSTANCE.getObjectMapper();
            ObjectNode result = objectMapper.createObjectNode();
            ObjectNode schemaObjectNode = objectMapper.createObjectNode();
            result.set("schema", schemaObjectNode);
            schemaObjectNode
                    .put("schemaName", schema.getName())
                    .put("createdOn", schemaVertex.value("createdOn").toString());
            return result;
        } else {
            throw new IllegalStateException(String.format("Unknown schema '%s'", schemaName));
        }
    }

    public static ObjectNode retrieveVertexEdgeLabelDetails(Request req) {
        SqlgGraph sqlgGraph = SqlgUI.INSTANCE.getSqlgGraph();
        String schemaName = req.params("schemaName");
        String abstractLabel = req.params("abstractLabel");
        String vertexOrEdge = req.params("vertexOrEdge");
        Optional<Schema> schemaOptional = sqlgGraph.getTopology().getSchema(schemaName);
        if (schemaOptional.isPresent()) {
            Schema schema = schemaOptional.get();
            List<Vertex> schemaVertices = sqlgGraph.topology().V()
                    .hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                    .has(Topology.SQLG_SCHEMA_SCHEMA_NAME, schema.getName())
                    .toList();
            Preconditions.checkState(schemaVertices.size() == 1);
            Vertex schemaVertex = schemaVertices.get(0);
            ObjectMapper objectMapper = ObjectMapperFactory.INSTANCE.getObjectMapper();
            ObjectNode result = objectMapper.createObjectNode();

            ObjectNode abstractLabelObjectNode = objectMapper.createObjectNode();
            result.set("abstractLabel", abstractLabelObjectNode);

            abstractLabelObjectNode.put("schemaName", schema.getName());

            ObjectNode identifierData = objectMapper.createObjectNode();
            ArrayNode identifiers = objectMapper.createArrayNode();
            identifierData.set("identifiers", identifiers);
            abstractLabelObjectNode.set("identifierData", identifierData);

            ObjectNode propertyColumnsGridData = objectMapper.createObjectNode();
            abstractLabelObjectNode.set("propertyColumns", propertyColumnsGridData);
            ArrayNode propertyColumnsColumns = objectMapper.createArrayNode();
            ArrayNode propertyColumnGridData = objectMapper.createArrayNode();
            propertyColumnsGridData.set("columns", propertyColumnsColumns);
            propertyColumnsGridData.set("data", propertyColumnGridData);
            propertyColumnsColumns.add(new SlickGridColumn.SlickGridColumnBuilder("name", "name", PropertyType.STRING)
                    .setMinWidth(220)
                    .build().toJson(objectMapper));
            propertyColumnsColumns.add(new SlickGridColumn.SlickGridColumnBuilder("type", "sqlg type", PropertyType.STRING)
                    .setMinWidth(220)
                    .build().toJson(objectMapper));
            propertyColumnsColumns.add(new SlickGridColumn.SlickGridColumnBuilder("sqlType", "sql type", PropertyType.STRING)
                    .setMinWidth(220)
                    .build().toJson(objectMapper));


            ObjectNode indexesGridData = objectMapper.createObjectNode();
            abstractLabelObjectNode.set("indexes", indexesGridData);
            ArrayNode indexColumns = objectMapper.createArrayNode();
            ArrayNode indexGridData = objectMapper.createArrayNode();
            indexesGridData.set("columns", indexColumns);
            indexesGridData.set("data", indexGridData);

            indexColumns.add(new SlickGridColumn.SlickGridColumnBuilder("name", "name", PropertyType.STRING)
                    .setMinWidth(80)
                    .build().toJson(objectMapper));
            indexColumns.add(new SlickGridColumn.SlickGridColumnBuilder("type", "type", PropertyType.STRING)
                    .setMinWidth(80)
                    .build().toJson(objectMapper));
            indexColumns.add(new SlickGridColumn.SlickGridColumnBuilder("properties", "properties", PropertyType.STRING)
                    .build().toJson(objectMapper));

            ObjectNode partitionsGrid = objectMapper.createObjectNode();
            abstractLabelObjectNode.set("partitions", partitionsGrid);
            ArrayNode partitionColumns = objectMapper.createArrayNode();
            ArrayNode partitionGridData = objectMapper.createArrayNode();
            partitionsGrid.set("columns", partitionColumns);
            partitionsGrid.set("data", partitionGridData);

            ObjectNode options = objectMapper.createObjectNode();
            partitionsGrid.set("options", options);
            options.put("isTree", true);
            options.put("deletionCheckBox", true);

            partitionColumns.add(new SlickGridColumn.SlickGridColumnBuilder("name", "name", PropertyType.STRING)
                    .setMinWidth(80)
                    .build().toJson(objectMapper));
            partitionColumns.add(new SlickGridColumn.SlickGridColumnBuilder("from", "from", PropertyType.STRING)
                    .setMinWidth(80)
                    .build().toJson(objectMapper));
            partitionColumns.add(new SlickGridColumn.SlickGridColumnBuilder("to", "to", PropertyType.STRING)
                    .setMinWidth(80)
                    .build().toJson(objectMapper));
            partitionColumns.add(new SlickGridColumn.SlickGridColumnBuilder("in", "in", PropertyType.STRING)
                    .setMinWidth(80)
                    .build().toJson(objectMapper));
            partitionColumns.add(new SlickGridColumn.SlickGridColumnBuilder("type", "type", PropertyType.STRING)
                    .setMinWidth(80)
                    .build().toJson(objectMapper));
            partitionColumns.add(new SlickGridColumn.SlickGridColumnBuilder("expression", "expression", PropertyType.STRING)
                    .setMinWidth(80)
                    .build().toJson(objectMapper));

            if (vertexOrEdge.equals("vertex")) {

                ObjectNode inEdgeLabelGrid = objectMapper.createObjectNode();
                abstractLabelObjectNode.set("inEdgeLabels", inEdgeLabelGrid);
                ArrayNode inEdgeLabelColumns = objectMapper.createArrayNode();
                ArrayNode inEdgeLabelGridData = objectMapper.createArrayNode();
                inEdgeLabelGrid.set("columns", inEdgeLabelColumns);
                inEdgeLabelGrid.set("data", inEdgeLabelGridData);
                inEdgeLabelColumns.add(new SlickGridColumn.SlickGridColumnBuilder("name", "name", PropertyType.STRING)
                        .setMinWidth(80)
                        .build().toJson(objectMapper));
                inEdgeLabelColumns.add(new SlickGridColumn.SlickGridColumnBuilder("schema", "schema", PropertyType.STRING)
                        .setMinWidth(80)
                        .build().toJson(objectMapper));

                ObjectNode outEdgeLabelGrid = objectMapper.createObjectNode();
                abstractLabelObjectNode.set("outEdgeLabels", outEdgeLabelGrid);
                ArrayNode outEdgeLabelColumns = objectMapper.createArrayNode();
                ArrayNode outEdgeLabelGridData = objectMapper.createArrayNode();
                outEdgeLabelGrid.set("columns", outEdgeLabelColumns);
                outEdgeLabelGrid.set("data", outEdgeLabelGridData);
                outEdgeLabelColumns.add(new SlickGridColumn.SlickGridColumnBuilder("name", "name", PropertyType.STRING)
                        .setMinWidth(80)
                        .build().toJson(objectMapper));
                outEdgeLabelColumns.add(new SlickGridColumn.SlickGridColumnBuilder("schema", "schema", PropertyType.STRING)
                        .setMinWidth(80)
                        .build().toJson(objectMapper));

                Optional<VertexLabel> vertexLabelOptional = schema.getVertexLabel(abstractLabel);
                if (vertexLabelOptional.isPresent()) {
                    VertexLabel vertexLabel = vertexLabelOptional.get();
                    abstractLabelObjectNode.put("label", "VertexLabel")
                            .put("name", vertexLabel.getName())
                            .put("ID", "TODO")
                            .put("createdOn", "TODO");
                    if (vertexLabel.hasIDPrimaryKey()) {
                        identifierData.put("userDefinedIdentifiers", false);
                        identifiers.add("ID");
                    } else {
                        identifierData.put("userDefinedIdentifiers", true);
                        for (String identifier : vertexLabel.getIdentifiers()) {
                            identifiers.add(identifier);
                        }
                    }
                    //Properties
                    for (PropertyColumn propertyColumn : vertexLabel.getProperties().values()) {
                        ObjectNode propertyColumnObjectNode = objectMapper.createObjectNode();
                        propertyColumnObjectNode.put("id", schemaName + "_" + vertexLabel.getName() + "_" + propertyColumn.getName());
                        propertyColumnObjectNode.put("name", propertyColumn.getName());
                        propertyColumnObjectNode.put("type", propertyColumn.getPropertyType().name());
                        ArrayNode sqlTypeArrayNode = objectMapper.createArrayNode();
                        String[] sqlType = sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyColumn.getPropertyType());
                        for (String s : sqlType) {
                            sqlTypeArrayNode.add(s);
                        }
                        propertyColumnObjectNode.set("sqlType", sqlTypeArrayNode);
                        propertyColumnGridData.add(propertyColumnObjectNode);
                    }
                    //Indexes
                    for (Index index : vertexLabel.getIndexes().values()) {
                        ObjectNode indexObjectNode = objectMapper.createObjectNode();
                        indexGridData.add(indexObjectNode);
                        indexObjectNode.put("id", schemaName + "_" + vertexLabel.getName() + "_" + index.getName());
                        indexObjectNode.put("name", index.getName());
                        indexObjectNode.put("type", index.getIndexType().getName());
                        indexObjectNode.put("properties", index.getProperties().stream().map(PropertyColumn::getName).reduce((a, b) -> a + "," + b).orElseThrow());
                    }
                    //In edge labels
                    for (String inEdgeLabelKey : vertexLabel.getInEdgeLabels().keySet()) {
                        EdgeLabel inEdgeLabel = vertexLabel.getInEdgeLabels().get(inEdgeLabelKey);
                        String edgeSchemaName = inEdgeLabel.getSchema().getName();
                        String edgeLabelName = inEdgeLabel.getLabel();
                        ObjectNode inEdgeObjectNode = objectMapper.createObjectNode();
                        inEdgeObjectNode.put("id", inEdgeLabel.getFullName());
                        inEdgeObjectNode.put("name", edgeLabelName);
                        inEdgeObjectNode.put("schema", edgeSchemaName);
                        inEdgeLabelGridData.add(inEdgeObjectNode);
                    }
                    //Out edge labels
                    for (String outEdgeLabelKey : vertexLabel.getOutEdgeLabels().keySet()) {
                        EdgeLabel outEdgeLabel = vertexLabel.getOutEdgeLabels().get(outEdgeLabelKey);
                        String edgeSchemaName = outEdgeLabel.getSchema().getName();
                        String edgeLabelName = outEdgeLabel.getLabel();
                        ObjectNode outEdgeObjectNode = objectMapper.createObjectNode();
                        outEdgeObjectNode.put("id", outEdgeLabel.getFullName());
                        outEdgeObjectNode.put("name", edgeLabelName);
                        outEdgeObjectNode.put("schema", edgeSchemaName);
                        outEdgeLabelGridData.add(outEdgeObjectNode);
                    }

                    //Partitions
                    abstractLabelObjectNode.put("partitionType", vertexLabel.getPartitionType().name());
                    abstractLabelObjectNode.put("partitionExpression", vertexLabel.getPartitionExpression());
                    ListOrderedSet<SlickLazyTree> roots = new ListOrderedSet<>();
                    ListOrderedSet<SlickLazyTree> allEntries = new ListOrderedSet<>();
                    Stack<String> parents = new Stack<>();
                    for (Partition partition : vertexLabel.getPartitions().values()) {
                        preparePartition(
                                objectMapper,
                                roots,
                                allEntries,
                                null,
                                parents,
                                partition
                        );
                    }
                    SlickLazyTreeContainer treeContainer = new SlickLazyTreeContainer(roots);
                    partitionGridData = treeContainer.complete(new PartitionSlickLazyTreeHelper(allEntries));
                    partitionsGrid.set("data", partitionGridData);
                } else {
                    throw new IllegalStateException(String.format("Unknown vertex label '%s'", abstractLabel));
                }
            } else {
                Optional<EdgeLabel> edgeLabelOptional = schema.getEdgeLabel(abstractLabel);
                if (edgeLabelOptional.isPresent()) {
                    EdgeLabel edgeLabel = edgeLabelOptional.get();
                    abstractLabelObjectNode.put("label", "EdgeLabel")
                            .put("name", edgeLabel.getName())
                            .put("ID", schemaVertex.id().toString())
                            .put("createdOn", schemaVertex.value("createdOn").toString());
                    if (edgeLabel.hasIDPrimaryKey()) {
                        identifierData.put("userDefinedIdentifiers", false);
                        identifiers.add("ID");
                    } else {
                        identifierData.put("userDefinedIdentifiers", true);
                        for (String identifier : edgeLabel.getIdentifiers()) {
                            identifiers.add(identifier);
                        }
                    }
                    for (PropertyColumn propertyColumn : edgeLabel.getProperties().values()) {
                        ObjectNode propertyColumnObjectNode = objectMapper.createObjectNode();
                        propertyColumnObjectNode.put("id", schemaName + "_" + edgeLabel.getName() + "_" + propertyColumn.getName());
                        propertyColumnObjectNode.put("name", propertyColumn.getName());
                        propertyColumnObjectNode.put("type", propertyColumn.getPropertyType().name());
                        ArrayNode sqlTypeArrayNode = objectMapper.createArrayNode();
                        String[] sqlType = sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyColumn.getPropertyType());
                        for (String s : sqlType) {
                            sqlTypeArrayNode.add(s);
                        }
                        propertyColumnObjectNode.set("sqlType", sqlTypeArrayNode);
                        propertyColumnGridData.add(propertyColumnObjectNode);
                    }
                    for (Index index : edgeLabel.getIndexes().values()) {
                        ObjectNode indexObjectNode = objectMapper.createObjectNode();
                        indexGridData.add(indexObjectNode);
                        indexObjectNode.put("id", schemaName + "_" + edgeLabel.getName() + "_" + index.getName());
                        indexObjectNode.put("name", index.getName());
                        indexObjectNode.put("type", index.getIndexType().getName());
                        indexObjectNode.put("properties", index.getProperties().stream().map(PropertyColumn::getName).reduce((a, b) -> a + "," + b).orElseThrow());
                    }

                    //Partitions
                    abstractLabelObjectNode.put("partitionType", edgeLabel.getPartitionType().name());
                    abstractLabelObjectNode.put("partitionExpression", edgeLabel.getPartitionExpression());
                    ListOrderedSet<SlickLazyTree> roots = new ListOrderedSet<>();
                    ListOrderedSet<SlickLazyTree> allEntries = new ListOrderedSet<>();
                    Stack<String> parents = new Stack<>();
                    for (Partition partition : edgeLabel.getPartitions().values()) {
                        preparePartition(
                                objectMapper,
                                roots,
                                allEntries,
                                null,
                                parents,
                                partition
                        );
                    }
                    SlickLazyTreeContainer treeContainer = new SlickLazyTreeContainer(roots);
                    partitionGridData = treeContainer.complete(new PartitionSlickLazyTreeHelper(allEntries));
                    partitionsGrid.set("data", partitionGridData);
                } else {
                    throw new IllegalStateException(String.format("Unknown vertex label '%s'", abstractLabel));
                }
            }
            return result;
        } else {
            throw new IllegalStateException(String.format("Unknown schema '%s'", schemaName));
        }
    }

    private static void preparePartition(
            ObjectMapper mapper,
            ListOrderedSet<SlickLazyTree> roots,
            ListOrderedSet<SlickLazyTree> allEntries,
            SlickLazyTree parentSlickLazyTree,
            Stack<String> parents,
            Partition partition) {

        ObjectNode partitionObjectNode = mapper.createObjectNode();
        partitionObjectNode.put("id", partition.getName());
        partitionObjectNode.put("name", partition.getName());
        partitionObjectNode.put("from", partition.getFrom());
        partitionObjectNode.put("to", partition.getTo());
        partitionObjectNode.put("in", partition.getIn());
        partitionObjectNode.put("type", partition.getPartitionType().name());
        partitionObjectNode.put("expression", partition.getPartitionExpression());

        partitionObjectNode.put("level", parents.size());
        partitionObjectNode.put("_collapsed", true);
        partitionObjectNode.put("isLeaf", partition.getPartitions().isEmpty());
        if (parentSlickLazyTree == null) {
            partitionObjectNode.putNull("parent");
            partitionObjectNode.set("parents", mapper.createArrayNode());
        } else {
            partitionObjectNode.put("parent", parentSlickLazyTree.getId());
            ArrayNode parentsArray = mapper.createArrayNode();
            for (String parent : parents) {
                parentsArray.add(parent);
            }
            partitionObjectNode.set("parents", parentsArray);
        }
        partitionObjectNode.set("children", mapper.createArrayNode());

        SlickLazyTree partitionSlickLazyTree = SlickLazyTree.from(partitionObjectNode);
        partitionSlickLazyTree.setChildrenIsLoaded(true);
        allEntries.add(partitionSlickLazyTree);

        if (parentSlickLazyTree == null) {
            roots.add(partitionSlickLazyTree);
        } else {
            partitionSlickLazyTree.setParent(parentSlickLazyTree);
            parentSlickLazyTree.addChild(partitionSlickLazyTree);
        }

        for (Partition partitionChild : partition.getPartitions().values()) {
            parents.add(partitionSlickLazyTree.getId());
            preparePartition(mapper, roots, allEntries, partitionSlickLazyTree, parents, partitionChild);
            parents.pop();
        }
    }

    public static class PartitionSlickLazyTreeHelper implements ISlickLazyTree {

        public PartitionSlickLazyTreeHelper(ListOrderedSet<SlickLazyTree> ignore) {
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

    public static ObjectNode deleteSchemas(Request req) {
        ObjectMapper objectMapper = ObjectMapperFactory.INSTANCE.getObjectMapper();
        String body = req.body();
        try {
            Set<String> schemasToRemove = new HashSet<>();
            ArrayNode schemas = (ArrayNode) objectMapper.readTree(body);
            SqlgGraph sqlgGraph = SqlgUI.INSTANCE.getSqlgGraph();
            for (JsonNode schemaName : schemas) {
                String schemaNameText = schemaName.asText();
                Preconditions.checkState(!schemaNameText.equals(sqlgGraph.getSqlDialect().getPublicSchema()), "The public ('%s') schema may not be deleted.", schemaName);
                schemasToRemove.add(schemaNameText);
            }
            Mono.just(schemasToRemove).subscribeOn(Schedulers.boundedElastic())
                    .subscribe((s) -> {
                        String schemaNames = s.stream().map(a -> "'" + a + "'").reduce((a, b) -> a + "," + b).orElse("");
                        for (String schemaName : s) {
                            Optional<Schema> schemaOptional = sqlgGraph.getTopology().getSchema(schemaName);
                            if (schemaOptional.isPresent()) {
                                Schema schema = schemaOptional.get();
                                schema.remove();
                            }
                        }
                        NotificationManager.INSTANCE.sendNotification(String.format("Start deleting schemas, [%s]", schemaNames));
                        sqlgGraph.tx().commit();
//                        NotificationManager.INSTANCE.sendRefreshTree("metaSchema");
                        NotificationManager.INSTANCE.sendNotification(String.format("Done deleting schemas, [%s]", schemaNames));
                    });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return objectMapper.createObjectNode();
    }

    public static ObjectNode deleteVertexLabels(Request req) {
        ObjectMapper objectMapper = ObjectMapperFactory.INSTANCE.getObjectMapper();
        String schemaName = req.params("schemaName");
        String body = req.body();
        try {
            Set<String> vertexLabelsToRemove = new HashSet<>();
            ArrayNode vertexLabelArrayNode = (ArrayNode) objectMapper.readTree(body);
            for (JsonNode vertexLabelNameJsonNode : vertexLabelArrayNode) {
                String vertexLabelName = vertexLabelNameJsonNode.asText();
                vertexLabelsToRemove.add(vertexLabelName);
            }
            Mono.just(Pair.of(schemaName, vertexLabelsToRemove)).subscribeOn(Schedulers.boundedElastic())
                    .subscribe((pair) -> {
                        Set<String> vertexLabelNames = pair.getRight();
                        String vertexLabelConcatenatedMessage = vertexLabelNames.stream().map(a -> "'" + a + "'").reduce((a, b) -> a + "," + b).orElse("");
                        NotificationManager.INSTANCE.sendNotification(String.format("Start deleting vertexLabels, [%s]", vertexLabelConcatenatedMessage));
                        SqlgGraph sqlgGraph = SqlgUI.INSTANCE.getSqlgGraph();
                        Optional<Schema> schemaOptional = sqlgGraph.getTopology().getSchema(pair.getLeft());
                        if (schemaOptional.isPresent()) {
                            for (String vertexLabelName : vertexLabelNames) {
                                Optional<VertexLabel> vertexLabelOptional = schemaOptional.get().getVertexLabel(vertexLabelName);
                                if (vertexLabelOptional.isPresent()) {
                                    VertexLabel vertexLabel = vertexLabelOptional.get();
                                    vertexLabel.remove();
                                }
                            }
                        }
                        sqlgGraph.tx().commit();
                        NotificationManager.INSTANCE.sendRefreshVertexLabels(pair.getLeft(), "Deleted vertex labels");
                        NotificationManager.INSTANCE.sendNotification(String.format("Done deleting vertexLabels, [%s]", vertexLabelConcatenatedMessage));
                    });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return objectMapper.createObjectNode();
    }

    public static ObjectNode deleteEdgeLabels(Request req) {
        ObjectMapper objectMapper = ObjectMapperFactory.INSTANCE.getObjectMapper();
        String schemaName = req.params("schemaName");
        String body = req.body();
        try {
            Set<String> edgeLabelsToRemove = new HashSet<>();
            ArrayNode edgeLabelArrayNode = (ArrayNode) objectMapper.readTree(body);
            for (JsonNode edgeLabelNameJsonNode : edgeLabelArrayNode) {
                String edgeLabelName = edgeLabelNameJsonNode.asText();
                edgeLabelsToRemove.add(edgeLabelName);
            }
            Mono.just(Pair.of(schemaName, edgeLabelsToRemove)).subscribeOn(Schedulers.boundedElastic())
                    .subscribe((pair) -> {
                        Set<String> edgeLabelNames = pair.getRight();
                        String edgeLabelConcatenatedMessage = edgeLabelNames.stream().map(a -> "'" + a + "'").reduce((a, b) -> a + "," + b).orElse("");
                        NotificationManager.INSTANCE.sendNotification(String.format("Start deleting edgeLabels, [%s]", edgeLabelConcatenatedMessage));
                        SqlgGraph sqlgGraph = SqlgUI.INSTANCE.getSqlgGraph();
                        Optional<Schema> schemaOptional = sqlgGraph.getTopology().getSchema(pair.getLeft());
                        if (schemaOptional.isPresent()) {
                            for (String edgeLabelName : edgeLabelNames) {
                                Optional<EdgeLabel> edgeLabelOptional = schemaOptional.get().getEdgeLabel(edgeLabelName);
                                if (edgeLabelOptional.isPresent()) {
                                    EdgeLabel edgeLabel = edgeLabelOptional.get();
                                    edgeLabel.remove();
                                }
                            }
                        }
                        sqlgGraph.tx().commit();
                        NotificationManager.INSTANCE.sendRefreshEdgeLabels(pair.getLeft(), "Deleted edge labels");
                        NotificationManager.INSTANCE.sendNotification(String.format("Done deleting edgeLabels, [%s]", edgeLabelConcatenatedMessage));
                    });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return objectMapper.createObjectNode();
    }

    public static ObjectNode deleteSchema(Request req) {
        ObjectMapper objectMapper = ObjectMapperFactory.INSTANCE.getObjectMapper();
        String schemaName = req.params("schemaName");
        SqlgGraph sqlgGraph = SqlgUI.INSTANCE.getSqlgGraph();
        Preconditions.checkState(!schemaName.equals(sqlgGraph.getSqlDialect().getPublicSchema()), "The public ('%s') schema may not be deleted.", schemaName);
        Mono.just(schemaName)
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe((s) -> {
                    NotificationManager.INSTANCE.sendNotification(String.format("Start deleting schema, '%s'", s));
                    Optional<Schema> schemaOptional = sqlgGraph.getTopology().getSchema(s);
                    if (schemaOptional.isPresent()) {
                        Schema schema = schemaOptional.get();
                        schema.remove();
                    }
                    sqlgGraph.tx().commit();
                    NotificationManager.INSTANCE.sendRefreshTree("metaSchema");
                    NotificationManager.INSTANCE.sendNotification(String.format("Done deleting schema, '%s'", s));
                });
        return objectMapper.createObjectNode();
    }

    public static ObjectNode deleteAbstractLabel(Request req) {
        ObjectMapper objectMapper = ObjectMapperFactory.INSTANCE.getObjectMapper();
        String schemaName = req.params("schemaName");
        String abstractLabel = req.params("abstractLabel");
        String vertexOrEdge = req.params("vertexOrEdge");
        Mono.just(new AbstractLabelHolder(schemaName, abstractLabel, vertexOrEdge))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe((abstractLabelHolder) -> {
                    NotificationManager.INSTANCE.sendNotification(
                            String.format(
                                    "Start deleting %s, [%s]",
                                    abstractLabelHolder.vertexOrEdge.equals("vertex") ? "VertexLabel" : "EdgeLabel",
                                    abstractLabelHolder.abstractLabel));
                    SqlgGraph sqlgGraph = SqlgUI.INSTANCE.getSqlgGraph();
                    Optional<Schema> schemaOptional = sqlgGraph.getTopology().getSchema(abstractLabelHolder.schemaName);
                    if (schemaOptional.isPresent()) {
                        Schema schema = schemaOptional.get();
                        if (abstractLabelHolder.vertexOrEdge.equals("vertex")) {
                            Optional<VertexLabel> vertexLabelOptional = schema.getVertexLabel(abstractLabelHolder.abstractLabel);
                            if (vertexLabelOptional.isPresent()) {
                                VertexLabel vertexLabel = vertexLabelOptional.get();
                                vertexLabel.remove();
                            }
                        } else {
                            Preconditions.checkState(abstractLabelHolder.vertexOrEdge.equals("edge"), "Expected 'edge'");
                            Optional<EdgeLabel> edgeLabelOptional = schema.getEdgeLabel(abstractLabelHolder.abstractLabel);
                            if (edgeLabelOptional.isPresent()) {
                                EdgeLabel edgeLabel = edgeLabelOptional.get();
                                edgeLabel.remove();
                            }
                        }
                    }
                    sqlgGraph.tx().commit();
                    NotificationManager.INSTANCE.sendRefreshTree(String.format("metaSchema_%s_%s", abstractLabelHolder.schemaName, abstractLabelHolder.vertexOrEdge));
                    NotificationManager.INSTANCE.sendNotification(
                            String.format(
                                    "Done deleting %s, [%s]",
                                    abstractLabelHolder.vertexOrEdge.equals("vertex") ? "VertexLabel" : "EdgeLabel",
                                    abstractLabelHolder.abstractLabel));
                });
        return objectMapper.createObjectNode();
    }

    public static ObjectNode deleteProperties(Request req) {
        ObjectMapper objectMapper = ObjectMapperFactory.INSTANCE.getObjectMapper();
        String schemaName = req.params("schemaName");
        String abstractLabel = req.params("abstractLabel");
        String vertexOrEdge = req.params("vertexOrEdge");
        String body = req.body();
        try {
            Set<String> propertiesToRemove = new HashSet<>();
            ArrayNode properties = (ArrayNode) objectMapper.readTree(body);
            for (JsonNode property : properties) {
                propertiesToRemove.add(property.asText());
            }
            Mono.just(new PropertyHolder(schemaName, abstractLabel, vertexOrEdge, propertiesToRemove))
                    .subscribeOn(Schedulers.boundedElastic())
                    .subscribe((propertyHolder -> {
                        SqlgGraph sqlgGraph = SqlgUI.INSTANCE.getSqlgGraph();
                        try {
                            NotificationManager.INSTANCE.sendNotification(
                                    String.format(
                                            "Start deleting properties, [%s]",
                                            propertyHolder.propertiesToRemove.stream().reduce((a, b) -> a + "," + b).orElse("")));
                            Optional<Schema> schemaOptional = sqlgGraph.getTopology().getSchema(propertyHolder.schemaName);
                            if (schemaOptional.isPresent()) {
                                Schema schema = schemaOptional.get();
                                AbstractLabel _abstractLabel;
                                switch (propertyHolder.vertexOrEdge) {
                                    case "vertex":
                                        Optional<VertexLabel> vertexLabelOptional = schema.getVertexLabel(propertyHolder.abstractLabel);
                                        if (vertexLabelOptional.isPresent()) {
                                            _abstractLabel = vertexLabelOptional.get();
                                        } else {
                                            throw new IllegalStateException(String.format("VertexLabel '%s' not found.", propertyHolder.abstractLabel));
                                        }
                                        break;
                                    case "edge":
                                        Optional<EdgeLabel> edgeLabelOptional = schema.getEdgeLabel(propertyHolder.abstractLabel);
                                        if (edgeLabelOptional.isPresent()) {
                                            _abstractLabel = edgeLabelOptional.get();
                                        } else {
                                            throw new IllegalStateException(String.format("EdgeLabel '%s' not found.", propertyHolder.abstractLabel));
                                        }
                                        break;
                                    default:
                                        throw new IllegalStateException("Unknown type, expected 'vertex' or 'edge', got " + propertyHolder.vertexOrEdge);
                                }
                                for (String property : propertyHolder.propertiesToRemove) {
                                    Optional<PropertyColumn> propertyColumnOptional = _abstractLabel.getProperty(property);
                                    //noinspection OptionalIsPresent
                                    if (propertyColumnOptional.isPresent()) {
                                        propertyColumnOptional.get().remove();
                                    }
                                }
                            }
                            sqlgGraph.tx().commit();
                            NotificationManager.INSTANCE.sendRefreshAbstractLabel(
                                    propertyHolder.schemaName,
                                    propertyHolder.abstractLabel,
                                    propertyHolder.vertexOrEdge,
                                    "Deleted properties successfully");
                            NotificationManager.INSTANCE.sendNotification(
                                    String.format(
                                            "Done deleting properties, [%s]",
                                            propertyHolder.propertiesToRemove.stream().reduce((a, b) -> a + "," + b).orElse("")));
                        } catch (Exception e) {
                            LOGGER.error("Failed to delete properties!", e);
                            String m = String.format(
                                    "Failed deleting properties, [%s], %s",
                                    propertyHolder.propertiesToRemove.stream().reduce((a, b) -> a + "," + b).orElse(""), e.getMessage());
                            NotificationManager.INSTANCE.sendRefreshAbstractLabel(
                                    propertyHolder.schemaName,
                                    propertyHolder.abstractLabel,
                                    propertyHolder.vertexOrEdge,
                                    m);
                            NotificationManager.INSTANCE.sendNotification(m);

                        } finally {
                            sqlgGraph.tx().rollback();
                        }
                    }));
            return objectMapper.createObjectNode();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static ObjectNode deleteInEdgeLabels(Request req) {
        ObjectMapper objectMapper = ObjectMapperFactory.INSTANCE.getObjectMapper();
        String schemaName = req.params("schemaName");
        String abstractLabel = req.params("abstractLabel");
        String body = req.body();
        try {
            Set<String> inEdgeLabelsToRemove = new HashSet<>();
            ArrayNode inEdgeLabelsArrayNode = (ArrayNode) objectMapper.readTree(body);
            for (JsonNode inEdgeLabelsJsonNode : inEdgeLabelsArrayNode) {
                inEdgeLabelsToRemove.add(inEdgeLabelsJsonNode.asText());
            }
            Mono.just(new EdgeLabelHolder(schemaName, abstractLabel, inEdgeLabelsToRemove))
                    .subscribeOn(Schedulers.boundedElastic())
                    .subscribe((inEdgeLabelHolder -> {
                        SqlgGraph sqlgGraph = SqlgUI.INSTANCE.getSqlgGraph();
                        try {
                            NotificationManager.INSTANCE.sendNotification(
                                    String.format(
                                            "Start deleting inEdgeLabels, [%s]",
                                            inEdgeLabelHolder.edgeLabelsToRemove.stream().reduce((a, b) -> a + "," + b).orElse("")));
                            Optional<Schema> schemaOptional = sqlgGraph.getTopology().getSchema(inEdgeLabelHolder.schemaName);
                            if (schemaOptional.isPresent()) {
                                Schema schema = schemaOptional.get();
                                VertexLabel vertexLabel;
                                Optional<VertexLabel> vertexLabelOptional = schema.getVertexLabel(inEdgeLabelHolder.abstractLabel);
                                if (vertexLabelOptional.isPresent()) {
                                    vertexLabel = vertexLabelOptional.get();
                                } else {
                                    throw new IllegalStateException(String.format("VertexLabel '%s' not found.", inEdgeLabelHolder.abstractLabel));
                                }
                                for (String inEdgeLabel : inEdgeLabelHolder.edgeLabelsToRemove) {
                                    for (EdgeRole edgeRole : vertexLabel.getInEdgeRoles().values()) {
                                        if (edgeRole.getEdgeLabel().getName().equals(inEdgeLabel)) {
                                            edgeRole.remove();
                                            break;
                                        }
                                    }
                                }
                            }
                            sqlgGraph.tx().commit();
                            NotificationManager.INSTANCE.sendRefreshAbstractLabel(
                                    inEdgeLabelHolder.schemaName,
                                    inEdgeLabelHolder.abstractLabel,
                                    "vertex",
                                    "Deleted inEdgeLabels successfully.");
                            NotificationManager.INSTANCE.sendNotification(
                                    String.format(
                                            "Done deleting inEdgeLabels, [%s]",
                                            inEdgeLabelHolder.edgeLabelsToRemove.stream().reduce((a, b) -> a + "," + b).orElse("")));
                        } catch (Exception e) {
                            LOGGER.error("Failed to delete inEdgeLabels!", e);
                            NotificationManager.INSTANCE.sendRefreshAbstractLabel(
                                    inEdgeLabelHolder.schemaName,
                                    inEdgeLabelHolder.abstractLabel,
                                    "vertex",
                                    "Failed to delete inEdgeLabels");
                            NotificationManager.INSTANCE.sendNotification(
                                    String.format(
                                            "Failed deleting inEdgeLabels, [%s], %s",
                                            inEdgeLabelHolder.edgeLabelsToRemove.stream().reduce((a, b) -> a + "," + b).orElse(""), e.getMessage()));
                        } finally {
                            sqlgGraph.tx().rollback();
                        }
                    }));
            return objectMapper.createObjectNode();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static ObjectNode deleteOutEdgeLabels(Request req) {
        ObjectMapper objectMapper = ObjectMapperFactory.INSTANCE.getObjectMapper();
        String schemaName = req.params("schemaName");
        String abstractLabel = req.params("abstractLabel");
        String body = req.body();
        try {
            Set<String> outEdgeLabelsToRemove = new HashSet<>();
            ArrayNode outEdgeLabelsArrayNode = (ArrayNode) objectMapper.readTree(body);
            for (JsonNode outEdgeLabelsJsonNode : outEdgeLabelsArrayNode) {
                outEdgeLabelsToRemove.add(outEdgeLabelsJsonNode.asText());
            }
            Mono.just(new EdgeLabelHolder(schemaName, abstractLabel, outEdgeLabelsToRemove))
                    .subscribeOn(Schedulers.boundedElastic())
                    .subscribe((edgeLabelHolder -> {
                        SqlgGraph sqlgGraph = SqlgUI.INSTANCE.getSqlgGraph();
                        try {
                            NotificationManager.INSTANCE.sendNotification(
                                    String.format(
                                            "Start deleting inEdgeLabels, [%s]",
                                            edgeLabelHolder.edgeLabelsToRemove.stream().reduce((a, b) -> a + "," + b).orElse("")));
                            Optional<Schema> schemaOptional = sqlgGraph.getTopology().getSchema(edgeLabelHolder.schemaName);
                            if (schemaOptional.isPresent()) {
                                Schema schema = schemaOptional.get();
                                VertexLabel vertexLabel;
                                Optional<VertexLabel> vertexLabelOptional = schema.getVertexLabel(edgeLabelHolder.abstractLabel);
                                if (vertexLabelOptional.isPresent()) {
                                    vertexLabel = vertexLabelOptional.get();
                                } else {
                                    throw new IllegalStateException(String.format("VertexLabel '%s' not found.", edgeLabelHolder.abstractLabel));
                                }
                                for (String outEdgeLabel : edgeLabelHolder.edgeLabelsToRemove) {
                                    for (EdgeRole edgeRole : vertexLabel.getOutEdgeRoles().values()) {
                                        if (edgeRole.getEdgeLabel().getName().equals(outEdgeLabel)) {
                                            edgeRole.remove();
                                            break;
                                        }
                                    }
                                }
                            }
                            sqlgGraph.tx().commit();
                            NotificationManager.INSTANCE.sendRefreshAbstractLabel(
                                    edgeLabelHolder.schemaName,
                                    edgeLabelHolder.abstractLabel,
                                    "vertex",
                                    "Deleted inEdgeLabels successfully.");
                            NotificationManager.INSTANCE.sendNotification(
                                    String.format(
                                            "Done deleting inEdgeLabels, [%s]",
                                            edgeLabelHolder.edgeLabelsToRemove.stream().reduce((a, b) -> a + "," + b).orElse("")));
                        } catch (Exception e) {
                            LOGGER.error("Failed to delete inEdgeLabels!", e);
                            NotificationManager.INSTANCE.sendRefreshAbstractLabel(
                                    edgeLabelHolder.schemaName,
                                    edgeLabelHolder.abstractLabel,
                                    "vertex",
                                    "Failed to delete inEdgeLabels");
                            NotificationManager.INSTANCE.sendNotification(
                                    String.format(
                                            "Failed deleting inEdgeLabels, [%s], %s",
                                            edgeLabelHolder.edgeLabelsToRemove.stream().reduce((a, b) -> a + "," + b).orElse(""), e.getMessage()));
                        } finally {
                            sqlgGraph.tx().rollback();
                        }
                    }));
            return objectMapper.createObjectNode();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static ObjectNode deleteIndexes(Request req) {
        ObjectMapper objectMapper = ObjectMapperFactory.INSTANCE.getObjectMapper();
        String schemaName = req.params("schemaName");
        String abstractLabel = req.params("abstractLabel");
        String vertexOrEdge = req.params("vertexOrEdge");
        String body = req.body();
        try {
            Set<String> indexesToRemove = new HashSet<>();
            ArrayNode indexesArrayNode = (ArrayNode) objectMapper.readTree(body);
            for (JsonNode indexJsonNode : indexesArrayNode) {
                indexesToRemove.add(indexJsonNode.asText());
            }
            Mono.just(new IndexesHolder(schemaName, abstractLabel, vertexOrEdge, indexesToRemove))
                    .subscribeOn(Schedulers.boundedElastic())
                    .subscribe((indexesHolder -> {
                        SqlgGraph sqlgGraph = SqlgUI.INSTANCE.getSqlgGraph();
                        try {
                            NotificationManager.INSTANCE.sendNotification(
                                    String.format(
                                            "Start deleting indexes, [%s]",
                                            indexesHolder.indexesToRemove.stream().reduce((a, b) -> a + "," + b).orElse("")));
                            Optional<Schema> schemaOptional = sqlgGraph.getTopology().getSchema(indexesHolder.schemaName);
                            if (schemaOptional.isPresent()) {
                                Schema schema = schemaOptional.get();
                                AbstractLabel _abstractLabel;
                                switch (indexesHolder.vertexOrEdge) {
                                    case "vertex":
                                        Optional<VertexLabel> vertexLabelOptional = schema.getVertexLabel(indexesHolder.abstractLabel);
                                        if (vertexLabelOptional.isPresent()) {
                                            _abstractLabel = vertexLabelOptional.get();
                                        } else {
                                            throw new IllegalStateException(String.format("VertexLabel '%s' not found.", indexesHolder.abstractLabel));
                                        }
                                        break;
                                    case "edge":
                                        Optional<EdgeLabel> edgeLabelOptional = schema.getEdgeLabel(indexesHolder.abstractLabel);
                                        if (edgeLabelOptional.isPresent()) {
                                            _abstractLabel = edgeLabelOptional.get();
                                        } else {
                                            throw new IllegalStateException(String.format("EdgeLabel '%s' not found.", indexesHolder.abstractLabel));
                                        }
                                        break;
                                    default:
                                        throw new IllegalStateException("Unknown type, expected 'vertex' or 'edge', got " + indexesHolder.vertexOrEdge);
                                }
                                for (String index : indexesHolder.indexesToRemove) {
                                    Optional<Index> indexOptional = _abstractLabel.getIndex(index);
                                    //noinspection OptionalIsPresent
                                    if (indexOptional.isPresent()) {
                                        indexOptional.get().remove();
                                    }
                                }
                            }
                            sqlgGraph.tx().commit();
                            NotificationManager.INSTANCE.sendRefreshAbstractLabel(
                                    indexesHolder.schemaName,
                                    indexesHolder.abstractLabel,
                                    indexesHolder.vertexOrEdge,
                                    "Deleted indexes successfully.");
                            NotificationManager.INSTANCE.sendNotification(
                                    String.format(
                                            "Done deleting indexes, [%s]",
                                            indexesHolder.indexesToRemove.stream().reduce((a, b) -> a + "," + b).orElse("")));
                        } catch (Exception e) {
                            LOGGER.error("Failed to delete indexes!", e);
                            NotificationManager.INSTANCE.sendRefreshAbstractLabel(
                                    indexesHolder.schemaName,
                                    indexesHolder.abstractLabel,
                                    indexesHolder.vertexOrEdge,
                                    "Failed to delete indexes");
                            NotificationManager.INSTANCE.sendNotification(
                                    String.format(
                                            "Failed deleting indexes, [%s], %s",
                                            indexesHolder.indexesToRemove.stream().reduce((a, b) -> a + "," + b).orElse(""), e.getMessage()));
                        } finally {
                            sqlgGraph.tx().rollback();
                        }
                    }));
            return objectMapper.createObjectNode();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static ObjectNode deletePartitions(Request req) {
        ObjectMapper objectMapper = ObjectMapperFactory.INSTANCE.getObjectMapper();
        String schemaName = req.params("schemaName");
        String abstractLabel = req.params("abstractLabel");
        String vertexOrEdge = req.params("vertexOrEdge");
        String body = req.body();
        try {
            Set<String> partitionsToRemove = new HashSet<>();
            ArrayNode partitionsArrayNode = (ArrayNode) objectMapper.readTree(body);
            for (JsonNode partitionJsonNode : partitionsArrayNode) {
                partitionsToRemove.add(partitionJsonNode.asText());
            }
            Mono.just(new PartitionsHolder(schemaName, abstractLabel, vertexOrEdge, partitionsToRemove))
                    .subscribeOn(Schedulers.boundedElastic())
                    .subscribe((partitionsHolder -> {
                        SqlgGraph sqlgGraph = SqlgUI.INSTANCE.getSqlgGraph();
                        try {
                            NotificationManager.INSTANCE.sendNotification(
                                    String.format(
                                            "Start deleting partitions, [%s]",
                                            partitionsHolder.partitionsToRemove.stream().reduce((a, b) -> a + "," + b).orElse("")));
                            Optional<Schema> schemaOptional = sqlgGraph.getTopology().getSchema(partitionsHolder.schemaName);
                            if (schemaOptional.isPresent()) {
                                Schema schema = schemaOptional.get();
                                AbstractLabel _abstractLabel;
                                switch (partitionsHolder.vertexOrEdge) {
                                    case "vertex":
                                        Optional<VertexLabel> vertexLabelOptional = schema.getVertexLabel(partitionsHolder.abstractLabel);
                                        if (vertexLabelOptional.isPresent()) {
                                            _abstractLabel = vertexLabelOptional.get();
                                        } else {
                                            throw new IllegalStateException(String.format("VertexLabel '%s' not found.", partitionsHolder.abstractLabel));
                                        }
                                        break;
                                    case "edge":
                                        Optional<EdgeLabel> edgeLabelOptional = schema.getEdgeLabel(partitionsHolder.abstractLabel);
                                        if (edgeLabelOptional.isPresent()) {
                                            _abstractLabel = edgeLabelOptional.get();
                                        } else {
                                            throw new IllegalStateException(String.format("EdgeLabel '%s' not found.", partitionsHolder.abstractLabel));
                                        }
                                        break;
                                    default:
                                        throw new IllegalStateException("Unknown type, expected 'vertex' or 'edge', got " + partitionsHolder.vertexOrEdge);
                                }
                                for (String partition : partitionsHolder.partitionsToRemove) {
                                    Optional<Partition> partitionOptional = _abstractLabel.getPartition(partition);
                                    //noinspection OptionalIsPresent
                                    if (partitionOptional.isPresent()) {
                                        partitionOptional.get().remove();
                                    }
                                }
                            }
                            sqlgGraph.tx().commit();
                            NotificationManager.INSTANCE.sendRefreshAbstractLabel(
                                    partitionsHolder.schemaName,
                                    partitionsHolder.abstractLabel,
                                    partitionsHolder.vertexOrEdge,
                                    "Delete partitions successfully");
                            NotificationManager.INSTANCE.sendNotification(
                                    String.format(
                                            "Done deleting partitions, [%s]",
                                            partitionsHolder.partitionsToRemove.stream().reduce((a, b) -> a + "," + b).orElse("")));
                        } catch (Exception e) {
                            LOGGER.error("Failed to delete partitions!", e);
                            NotificationManager.INSTANCE.sendRefreshAbstractLabel(
                                    partitionsHolder.schemaName,
                                    partitionsHolder.abstractLabel,
                                    partitionsHolder.vertexOrEdge,
                                    "Failed to delete partitions");
                            NotificationManager.INSTANCE.sendNotification(
                                    String.format(
                                            "Failed deleting partitions, [%s], %s",
                                            partitionsHolder.partitionsToRemove.stream().reduce((a, b) -> a + "," + b).orElse(""), e.getMessage()));
                        } finally {
                            sqlgGraph.tx().rollback();
                        }
                    }));
            return objectMapper.createObjectNode();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static class AbstractLabelHolder {
        String schemaName;
        String abstractLabel;
        String vertexOrEdge;

        private AbstractLabelHolder(String schemaName, String abstractLabel, String vertexOrEdge) {
            this.schemaName = schemaName;
            this.abstractLabel = abstractLabel;
            this.vertexOrEdge = vertexOrEdge;
        }

        @Override
        public int hashCode() {
            return (this.schemaName + this.abstractLabel + this.vertexOrEdge).hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof AbstractLabelHolder)) {
                return false;
            }
            AbstractLabelHolder other = (AbstractLabelHolder) o;
            return this.schemaName.equals(other.schemaName) && this.abstractLabel.equals(other.abstractLabel) && this.vertexOrEdge.equals(other.vertexOrEdge);
        }
    }

    private static class PropertyHolder {
        String schemaName;
        String abstractLabel;
        String vertexOrEdge;
        Set<String> propertiesToRemove;
        String propertiesConcatenated;

        private PropertyHolder(String schemaName, String abstractLabel, String vertexOrEdge, Set<String> propertiesToRemove) {
            this.schemaName = schemaName;
            this.abstractLabel = abstractLabel;
            this.vertexOrEdge = vertexOrEdge;
            this.propertiesToRemove = propertiesToRemove;
            this.propertiesConcatenated = propertiesToRemove.stream().reduce((a, b) -> a + b).orElse("");
        }

        @Override
        public int hashCode() {
            return (this.schemaName + this.abstractLabel + this.vertexOrEdge + this.propertiesConcatenated).hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof PropertyHolder)) {
                return false;
            }
            PropertyHolder other = (PropertyHolder) o;
            return this.schemaName.equals(other.schemaName) && this.abstractLabel.equals(other.abstractLabel) &&
                    this.vertexOrEdge.equals(other.vertexOrEdge) && this.propertiesConcatenated.equals(other.propertiesConcatenated);
        }
    }

    private static class IndexesHolder {
        String schemaName;
        String abstractLabel;
        String vertexOrEdge;
        Set<String> indexesToRemove;
        String indexesConcatenated;

        private IndexesHolder(String schemaName, String abstractLabel, String vertexOrEdge, Set<String> indexesToRemove) {
            this.schemaName = schemaName;
            this.abstractLabel = abstractLabel;
            this.vertexOrEdge = vertexOrEdge;
            this.indexesToRemove = indexesToRemove;
            this.indexesConcatenated = indexesToRemove.stream().reduce((a, b) -> a + b).orElse("");
        }

        @Override
        public int hashCode() {
            return (this.schemaName + this.abstractLabel + this.vertexOrEdge + this.indexesConcatenated).hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof IndexesHolder)) {
                return false;
            }
            IndexesHolder other = (IndexesHolder) o;
            return this.schemaName.equals(other.schemaName) && this.abstractLabel.equals(other.abstractLabel) &&
                    this.vertexOrEdge.equals(other.vertexOrEdge) && this.indexesConcatenated.equals(other.indexesConcatenated);
        }
    }

    private static class EdgeLabelHolder {
        String schemaName;
        String abstractLabel;
        Set<String> edgeLabelsToRemove;
        String edgeLabelsConcatenated;

        private EdgeLabelHolder(String schemaName, String abstractLabel, Set<String> edgeLabelsToRemove) {
            this.schemaName = schemaName;
            this.abstractLabel = abstractLabel;
            this.edgeLabelsToRemove = edgeLabelsToRemove;
            this.edgeLabelsConcatenated = edgeLabelsToRemove.stream().reduce((a, b) -> a + b).orElse("");
        }

        @Override
        public int hashCode() {
            return (this.schemaName + this.abstractLabel + this.edgeLabelsConcatenated).hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof IndexesHolder)) {
                return false;
            }
            EdgeLabelHolder other = (EdgeLabelHolder) o;
            return this.schemaName.equals(other.schemaName) && this.abstractLabel.equals(other.abstractLabel) &&
                    this.edgeLabelsConcatenated.equals(other.edgeLabelsConcatenated);
        }
    }

    private static class PartitionsHolder {
        String schemaName;
        String abstractLabel;
        String vertexOrEdge;
        Set<String> partitionsToRemove;
        String partitionsConcatenated;

        private PartitionsHolder(String schemaName, String abstractLabel, String vertexOrEdge, Set<String> partitionsToRemove) {
            this.schemaName = schemaName;
            this.abstractLabel = abstractLabel;
            this.vertexOrEdge = vertexOrEdge;
            this.partitionsToRemove = partitionsToRemove;
            this.partitionsConcatenated = partitionsToRemove.stream().reduce((a, b) -> a + b).orElse("");
        }

        @Override
        public int hashCode() {
            return (this.schemaName + this.abstractLabel + this.vertexOrEdge + this.partitionsConcatenated).hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof PartitionsHolder)) {
                return false;
            }
            PartitionsHolder other = (PartitionsHolder) o;
            return this.schemaName.equals(other.schemaName) && this.abstractLabel.equals(other.abstractLabel) &&
                    this.vertexOrEdge.equals(other.vertexOrEdge) && this.partitionsConcatenated.equals(other.partitionsConcatenated);
        }
    }
}
