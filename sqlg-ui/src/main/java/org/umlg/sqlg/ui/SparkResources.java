package org.umlg.sqlg.ui;

import static spark.Spark.get;
import static spark.Spark.staticFiles;

public class SparkResources {

    public static void resources() {
        staticFiles.externalLocation("sqlg-ui/src/main/web");
        get("/hello", (req, res) -> "Hello World");
        get("/sqlg/data/v1/schema", (req, res) -> SchemaResource.retrieveSchemas(req));
    }
}
