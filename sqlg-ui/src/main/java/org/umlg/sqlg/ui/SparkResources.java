package org.umlg.sqlg.ui;

import com.auth0.jwt.interfaces.DecodedJWT;
import org.apache.commons.text.StringEscapeUtils;
import spark.servlet.SparkApplication;

import static spark.Spark.*;

public class SparkResources implements SparkApplication {

    @Override
    public void init() {
        staticResources();
        resources();
        after("/sqlg/*", (request, response) -> {
            SqlgUI.INSTANCE.getSqlgGraph().tx().rollback();
        });
    }

    public static void websocket() {
        webSocket("/sqlg/data/v1/websocket", SqlgWebsocket.class);
        after("/sqlg/*", (request, response) -> {
            response.header("Content-Encoding", "gzip");
            SqlgUI.INSTANCE.getSqlgGraph().tx().rollback();
        });
    }

    public static void staticResources() {
        staticFiles.location("/dist");
    }

    public static void resources() {
        get("/sqlg/data/v1/topologyTree", (req, res) -> SchemaResource.retrieveTopologyTree(req));
        get("/sqlg/data/v1/schemas", (req, res) -> SchemaResource.retrieveSchemas(req));
        get("/sqlg/data/v1/schemas/:schemaName/vertexLabels", (req, res) -> SchemaResource.retrieveVertexLabels(req));
        get("/sqlg/data/v1/schemas/:schemaName/edgeLabels", (req, res) -> SchemaResource.retrieveEdgeLabels(req));
        get("/sqlg/data/v1/schema/:schemaName", (req, res) -> SchemaResource.retrieveSchemaDetails(req));
        get("/sqlg/data/v1/schema/:schemaName/:abstractLabel/:vertexOrEdge", (req, res) -> SchemaResource.retrieveVertexEdgeLabelDetails(req));
        get("/sqlg/data/v1/graph", (req, res) -> SchemaResource.retrieveGraph());
        post("/sqlg/data/v1/login", SchemaResource::login);
        get("/sqlg/data/v1/userAllowedToEdit", SchemaResource::userAllowedToEdit);
        delete("/sqlg/data/v1/delete/schema/:schemaName", (req, res) -> SchemaResource.deleteSchema(req));
        delete("/sqlg/data/v1/delete/schema/:schemaName/:abstractLabel/:vertexOrEdge", (req, res) -> SchemaResource.deleteAbstractLabel(req));
        delete("/sqlg/data/v1/delete/schema/:schemaName/:abstractLabel/:vertexOrEdge/properties", (req, res) -> SchemaResource.deleteProperties(req));
        delete("/sqlg/data/v1/delete/schema/:schemaName/:abstractLabel/:vertexOrEdge/indexes", (req, res) -> SchemaResource.deleteIndexes(req));
        delete("/sqlg/data/v1/delete/schema/:schemaName/:abstractLabel/vertex/inEdgeLabels", (req, res) -> SchemaResource.deleteInEdgeLabels(req));
        delete("/sqlg/data/v1/delete/schema/:schemaName/:abstractLabel/vertex/outEdgeLabels", (req, res) -> SchemaResource.deleteOutEdgeLabels(req));
        delete("/sqlg/data/v1/delete/schema/:schemaName/:abstractLabel/:vertexOrEdge/partitions", (req, res) -> SchemaResource.deletePartitions(req));
        delete("/sqlg/data/v1/delete/schema", (req, res) -> SchemaResource.deleteSchemas(req));
        delete("/sqlg/data/v1/delete/schema/:schemaName/vertexLabels", (req, res) -> SchemaResource.deleteVertexLabels(req));
        delete("/sqlg/data/v1/delete/schema/:schemaName/edgeLabels", (req, res) -> SchemaResource.deleteEdgeLabels(req));
        before("/sqlg/*", (req, res) -> {
            String token = req.cookie(AuthUtil.SQLG_TOKEN);
            if (token != null) {
                DecodedJWT jwt = AuthUtil.validToken(token);
                if (jwt != null) {
                    res.cookie("/", AuthUtil.SQLG_TOKEN, token, SqlgUI.INSTANCE.getSqlgGraph().configuration().getInt("sqlg.ui.cookie.expiry", 3600), true, false);
                    before("/sqlg/data/v1/delete/*", (request, response) -> {
                                String username = jwt.getClaim("username").asString();
                                String edit = "sqlg.ui.username." + username + ".edit";
                                if (!SqlgUI.INSTANCE.getSqlgGraph().configuration().getBoolean(edit)) {
                                    halt(401, "Edit not allowed.");
                                }
                            }
                    );
                } else {
                    res.removeCookie("/", AuthUtil.SQLG_TOKEN);
                }
            } else {
                res.removeCookie("/", AuthUtil.SQLG_TOKEN);
            }
        });
        exception(Exception.class, (exception, request, response) -> {
            SqlgUI.INSTANCE.getSqlgGraph().tx().rollback();
            response.status(500);
            response.body("\"" + StringEscapeUtils.escapeJson(exception.getMessage()) + "\"");
        });
    }

}
