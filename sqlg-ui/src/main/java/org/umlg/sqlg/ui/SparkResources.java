package org.umlg.sqlg.ui;

import com.auth0.jwt.interfaces.DecodedJWT;
import org.apache.commons.text.StringEscapeUtils;
import spark.Service;
import spark.servlet.SparkApplication;

import static spark.Spark.*;

public class SparkResources implements SparkApplication {

    @Override
    public void init() {
        Service http = Service.ignite();
        staticResources(http);
        resources(http);
        http.after("/sqlg/*", (request, response) -> {
            SqlgUI.INSTANCE.getSqlgGraph().tx().rollback();
        });
    }

    public static void websocket(Service http) {
        http.webSocket("/sqlg/data/v1/websocket", SqlgWebsocket.class);
        http.after("/sqlg/*", (request, response) -> {
            response.header("Content-Encoding", "gzip");
            SqlgUI.INSTANCE.getSqlgGraph().tx().rollback();
        });
    }

    public static void staticResources(Service http) {
//        http.staticFiles.externalLocation("sqlg-ui/src/main/web/dist/dist");
        http.staticFiles.location("/dist");
    }

    public static void resources(Service http) {
        http.get("/sqlg/data/v1/topologyTree", (req, res) -> SchemaResource.retrieveTopologyTree(req));
        http.get("/sqlg/data/v1/schemas", (req, res) -> SchemaResource.retrieveSchemas(req));
        http.get("/sqlg/data/v1/schemas/:schemaName/vertexLabels", (req, res) -> SchemaResource.retrieveVertexLabels(req));
        http.get("/sqlg/data/v1/schemas/:schemaName/edgeLabels", (req, res) -> SchemaResource.retrieveEdgeLabels(req));
        http.get("/sqlg/data/v1/schema/:schemaName", (req, res) -> SchemaResource.retrieveSchemaDetails(req));
        http.get("/sqlg/data/v1/schema/:schemaName/:abstractLabel/:vertexOrEdge", (req, res) -> SchemaResource.retrieveVertexEdgeLabelDetails(req));
        http.get("/sqlg/data/v1/graph", (req, res) -> SchemaResource.retrieveGraph());
        http.post("/sqlg/data/v1/login", SchemaResource::login);
        http.get("/sqlg/data/v1/userAllowedToEdit", SchemaResource::userAllowedToEdit);
        http.delete("/sqlg/data/v1/delete/schema/:schemaName", (req, res) -> SchemaResource.deleteSchema(req));
        http.delete("/sqlg/data/v1/delete/schema/:schemaName/:abstractLabel/:vertexOrEdge", (req, res) -> SchemaResource.deleteAbstractLabel(req));
        http.delete("/sqlg/data/v1/delete/schema/:schemaName/:abstractLabel/:vertexOrEdge/properties", (req, res) -> SchemaResource.deleteProperties(req));
        http.delete("/sqlg/data/v1/delete/schema/:schemaName/:abstractLabel/:vertexOrEdge/indexes", (req, res) -> SchemaResource.deleteIndexes(req));
        http.delete("/sqlg/data/v1/delete/schema/:schemaName/:abstractLabel/vertex/inEdgeLabels", (req, res) -> SchemaResource.deleteInEdgeLabels(req));
        http.delete("/sqlg/data/v1/delete/schema/:schemaName/:abstractLabel/vertex/outEdgeLabels", (req, res) -> SchemaResource.deleteOutEdgeLabels(req));
        http.delete("/sqlg/data/v1/delete/schema/:schemaName/:abstractLabel/:vertexOrEdge/partitions", (req, res) -> SchemaResource.deletePartitions(req));
        http.delete("/sqlg/data/v1/delete/schema", (req, res) -> SchemaResource.deleteSchemas(req));
        http.delete("/sqlg/data/v1/delete/schema/:schemaName/vertexLabels", (req, res) -> SchemaResource.deleteVertexLabels(req));
        http.delete("/sqlg/data/v1/delete/schema/:schemaName/edgeLabels", (req, res) -> SchemaResource.deleteEdgeLabels(req));
        http.put("/sqlg/data/v1/rename/schema/:schemaName/:vertexLabel/:newVertexLabel", (req, res) -> SchemaResource.renameVertexLabel(req));
        http.put("/sqlg/data/v1/rename/schema/:schemaName/:abstractLabel/:vertexOrEdge/properties", (req, res) -> SchemaResource.renameProperties(req));
        http.before("/sqlg/*", (req, res) -> {
            String token = req.cookie(AuthUtil.SQLG_TOKEN);
            if (token != null) {
                DecodedJWT jwt = AuthUtil.validToken(token);
                if (jwt != null) {
                    res.cookie("/", AuthUtil.SQLG_TOKEN, token, SqlgUI.INSTANCE.getSqlgGraph().configuration().getInt("sqlg.ui.cookie.expiry", 3600), true, false);
                    http.before("/sqlg/data/v1/delete/*", (request, response) -> {
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
        http.exception(Exception.class, (exception, request, response) -> {
            SqlgUI.INSTANCE.getSqlgGraph().tx().rollback();
            response.status(500);
            response.body("\"" + StringEscapeUtils.escapeJson(exception.getMessage()) + "\"");
        });
    }

}
