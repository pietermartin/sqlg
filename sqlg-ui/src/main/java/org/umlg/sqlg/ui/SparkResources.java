package org.umlg.sqlg.ui;

import static spark.Spark.get;
import static spark.Spark.staticFiles;

public class SparkResources {

    public static void resources() {
        staticFiles.externalLocation("sqlg-ui/src/main/web");
        get("/sqlg/data/v1/schema", (req, res) -> SchemaResource.retrieveSchemas());
        get("/sqlg/data/v1/schema/:schemaName", (req, res) -> SchemaResource.retrieveSchemaDetails(req));
        get("/sqlg/data/v1/schema/:schemaName/:abstractLabel/:vertexOrEdge", (req, res) -> SchemaResource.retrieveVertexEdgeLabelDetails(req));
        get("/sqlg/data/v1/graph", (req, res) -> SchemaResource.retrieveGraph());
    }
}
