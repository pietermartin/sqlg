package org.umlg.sqlg.ui;

import io.javalin.Javalin;
import io.javalin.http.staticfiles.Location;
import org.umlg.sqlg.structure.SqlgGraph;

public class SqlgUI {

    private SqlgGraph sqlgGraph;
    public static SqlgUI INSTANCE = null;

    public static void initialize(int port) {
        if (INSTANCE == null) {
            var app = Javalin.create(config -> {
                        config.staticFiles.add(staticFiles -> {
//                            staticFiles.hostedPath = "/home/pieter/Projects/sqlg/";
                            staticFiles.directory = "/home/pieter/Projects/sqlg/sqlg-ui/src/main/web/dist/dist";
                            staticFiles.location = Location.EXTERNAL;
                        });
                    });
            SparkResources.resources(app);
            app.start(7070);
//            app.get("/path/*", ctx -> { // will match anything starting with /path/
//                ctx.result("You are here because " + ctx.path() + " matches " + ctx.matchedPath());
//            });

//            Service http = Service.ignite();
//            SparkResources.staticResources(http);
//            if (port != -1) {
//                http.port(port);
//            }
//            SparkResources.websocket(http);
//            SparkResources.resources(http);
//            http.awaitInitialization();
        }
    }

    public static void initialize() {
        initialize(-1);
    }

    public static void set(SqlgGraph sqlgGraph) {
        if (INSTANCE == null) {
            INSTANCE = new SqlgUI();
        }
        INSTANCE.setSqlgGraph(sqlgGraph);
    }

    public static void stop() {
        INSTANCE = null;
    }

    private SqlgUI() {
    }

    public SqlgGraph getSqlgGraph() {
        return sqlgGraph;
    }

    private void setSqlgGraph(SqlgGraph sqlgGraph) {
        this.sqlgGraph = sqlgGraph;
    }

}
