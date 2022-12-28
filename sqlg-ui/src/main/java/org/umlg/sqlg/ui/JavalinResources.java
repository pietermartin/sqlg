package org.umlg.sqlg.ui;

import io.javalin.Javalin;

import static io.javalin.apibuilder.ApiBuilder.*;

public class JavalinResources /*implements SparkApplication*/ {

//    @Override
//    public void init() {
//        Service http = Service.ignite();
//        staticResources(http);
//        resources(http);
//        http.after("/sqlg/*", (request, response) -> {
//            SqlgUI.INSTANCE.getSqlgGraph().tx().rollback();
//        });
//    }
//
//    public static void websocket(Service http) {
//        http.webSocket("/sqlg/data/v1/websocket", SqlgWebsocket.class);
//        http.after("/sqlg/*", (request, response) -> {
//            response.header("Content-Encoding", "gzip");
//            SqlgUI.INSTANCE.getSqlgGraph().tx().rollback();
//        });
//   }
//
//    public static void staticResources(Service http) {
////        http.staticFiles.externalLocation("sqlg-ui/src/main/web/dist/dist");
//        http.staticFiles.location("/dist");
//    }

    public static void resources(Javalin app) {

        app.routes(() -> {
            path("sqlg/data/v1", () -> {
                get("topologyTree", SchemaResource::retrieveTopologyTree);
                post("login", SchemaResource::login);
                get("schemas", SchemaResource::retrieveSchemas);
                get("schemas/{schemaName}/vertexLabels", SchemaResource::retrieveVertexLabels);
                get("schemas/{schemaName}/edgeLabels", SchemaResource::retrieveEdgeLabels);
                get("schema/{schemaName}", SchemaResource::retrieveSchemaDetails);
                get("schema/{schemaName}/{abstractLabel}/{vertexOrEdge}", SchemaResource::retrieveVertexEdgeLabelDetails);
                get("graph", SchemaResource::retrieveGraph);
                get("userAllowedToEdit", SchemaResource::userAllowedToEdit);
                delete("delete/schema/{schemaName}", SchemaResource::deleteSchema);
                delete("delete/schema/{schemaName}/{abstractLabel}/{vertexOrEdge}", SchemaResource::deleteAbstractLabel);
                delete("delete/schema/{schemaName}/{abstractLabel}/{vertexOrEdge}/properties", SchemaResource::deleteProperties);
                delete("delete/schema/{schemaName}/{abstractLabel}/{vertexOrEdge}/indexes", SchemaResource::deleteIndexes);
                delete("delete/schema/{schemaName}/{abstractLabel}/vertex/inEdgeLabels", SchemaResource::deleteInEdgeLabels);
                delete("delete/schema/{schemaName}/{abstractLabel}/vertex/outEdgeLabels", SchemaResource::deleteOutEdgeLabels);
                delete("delete/schema/{schemaName}/{abstractLabel}/{vertexOrEdge}/partitions", SchemaResource::deletePartitions);
                delete("delete/schema", SchemaResource::deleteSchemas);
                delete("delete/schema/{schemaName}/vertexLabels", SchemaResource::deleteVertexLabels);
                delete("delete/schema/{schemaName}/edgeLabels", SchemaResource::deleteEdgeLabels);
                put("rename/schema/{schemaName}/{vertexLabel}/{newVertexLabel}", SchemaResource::renameVertexLabel);
                put("rename/schema/{schemaName}/{abstractLabel}/{vertexOrEdge}/properties", SchemaResource::renameProperties);
            });
        });

//        http.before("/sqlg/*", (req, res) -> {
//            String token = req.cookie(AuthUtil.SQLG_TOKEN);
//            if (token != null) {
//                DecodedJWT jwt = AuthUtil.validToken(token);
//                if (jwt != null) {
//                    res.cookie("/", AuthUtil.SQLG_TOKEN, token, SqlgUI.INSTANCE.getSqlgGraph().configuration().getInt("sqlg.ui.cookie.expiry", 3600), true, false);
//                    http.before("/sqlg/data/v1/delete/*", (request, response) -> {
//                                String username = jwt.getClaim("username").asString();
//                                String edit = "sqlg.ui.username." + username + ".edit";
//                                if (!SqlgUI.INSTANCE.getSqlgGraph().configuration().getBoolean(edit)) {
//                                    halt(401, "Edit not allowed.");
//                                }
//                            }
//                    );
//                } else {
//                    res.removeCookie("/", AuthUtil.SQLG_TOKEN);
//                }
//            } else {
//                res.removeCookie("/", AuthUtil.SQLG_TOKEN);
//            }
//        });
//        http.exception(Exception.class, (exception, request, response) -> {
//            SqlgUI.INSTANCE.getSqlgGraph().tx().rollback();
//            response.status(500);
//            response.body("\"" + StringEscapeUtils.escapeJson(exception.getMessage()) + "\"");
//        });
    }

}
