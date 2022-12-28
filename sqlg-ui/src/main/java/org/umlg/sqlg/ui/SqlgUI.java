package org.umlg.sqlg.ui;

import com.google.common.base.Preconditions;
import io.javalin.Javalin;
import io.javalin.http.staticfiles.Location;
import org.umlg.sqlg.structure.SqlgGraph;

public class SqlgUI {

    private SqlgGraph sqlgGraph;
    public static SqlgUI INSTANCE = null;

    public static void initialize(SqlgGraph sqlgGraph, int port) {
        Preconditions.checkState(INSTANCE == null);
        set(sqlgGraph);
        var app = Javalin.create(config -> {
            config.staticFiles.add(staticFiles -> {
                staticFiles.directory = "/home/pieter/Projects/sqlg/sqlg-ui/src/main/web/dist/dist";
                staticFiles.location = Location.EXTERNAL;
            });
            config.compression.gzipOnly();
        });
        app.after(ctx -> SqlgUI.INSTANCE.getSqlgGraph().tx().rollback());
        JavalinResources.resources(app);
        app.ws("/sqlg/data/v1/websocket", ws -> {
            ws.onConnect(ctx -> NotificationManager.INSTANCE.add(ctx.session));
            ws.onClose(ctx -> NotificationManager.INSTANCE.remove(ctx.session));
        });
        app.wsAfter(wsConfig -> SqlgUI.INSTANCE.getSqlgGraph().tx().rollback());
        app.start(port);
    }

    public static void initialize(SqlgGraph sqlgGraph) {
        initialize(sqlgGraph, 8181);
    }

    private static void set(SqlgGraph sqlgGraph) {
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
