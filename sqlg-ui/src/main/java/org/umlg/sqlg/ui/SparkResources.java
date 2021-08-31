package org.umlg.sqlg.ui;

import static spark.Spark.*;

public class SparkResources {

    public static void resources() {
        webSocket("/sqlg/data/v1/websocket", SqlgWebsocket.class);
        staticFiles.externalLocation("sqlg-ui/src/main/web");
        get("/sqlg/data/v1/topologyTree", (req, res) -> SchemaResource.retrieveTopologyTree(req));
        get("/sqlg/data/v1/schemas", (req, res) -> SchemaResource.retrieveSchemas(req));
        get("/sqlg/data/v1/schemas/:schemaName/vertexLabels", (req, res) -> SchemaResource.retrieveVertexLabels(req));
        get("/sqlg/data/v1/schemas/:schemaName/edgeLabels", (req, res) -> SchemaResource.retrieveEdgeLabels(req));
        get("/sqlg/data/v1/schema/:schemaName", (req, res) -> SchemaResource.retrieveSchemaDetails(req));
        get("/sqlg/data/v1/schema/:schemaName/:abstractLabel/:vertexOrEdge", (req, res) -> SchemaResource.retrieveVertexEdgeLabelDetails(req));
        delete("/sqlg/data/v1/schema/:schemaName", (req, res) -> SchemaResource.deleteSchema(req));
        delete("/sqlg/data/v1/schema/:schemaName/:abstractLabel/:vertexOrEdge", (req, res) -> SchemaResource.deleteAbstractLabel(req));
        delete("/sqlg/data/v1/schema/:schemaName/:abstractLabel/:vertexOrEdge/properties", (req, res) -> SchemaResource.deleteProperties(req));
        delete("/sqlg/data/v1/schema/:schemaName/:abstractLabel/:vertexOrEdge/indexes", (req, res) -> SchemaResource.deleteIndexes(req));
        delete("/sqlg/data/v1/schema/:schemaName/:abstractLabel/vertex/inEdgeLabels", (req, res) -> SchemaResource.deleteInEdgeLabels(req));
        delete("/sqlg/data/v1/schema/:schemaName/:abstractLabel/vertex/outEdgeLabels", (req, res) -> SchemaResource.deleteOutEdgeLabels(req));
        delete("/sqlg/data/v1/schema/:schemaName/:abstractLabel/:vertexOrEdge/partitions", (req, res) -> SchemaResource.deletePartitions(req));
        delete("/sqlg/data/v1/schema", (req, res) -> SchemaResource.deleteSchemas(req));
        delete("/sqlg/data/v1/schema/:schemaName/vertexLabels", (req, res) -> SchemaResource.deleteVertexLabels(req));
        delete("/sqlg/data/v1/schema/:schemaName/edgeLabels", (req, res) -> SchemaResource.deleteEdgeLabels(req));
        get("/sqlg/data/v1/graph", (req, res) -> SchemaResource.retrieveGraph());
    }
}
