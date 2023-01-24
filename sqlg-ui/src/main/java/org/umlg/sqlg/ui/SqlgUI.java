package org.umlg.sqlg.ui;

import com.google.common.base.Preconditions;
import io.javalin.Javalin;
import io.javalin.http.staticfiles.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.SqlgGraph;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.stream.Collectors;

public class SqlgUI {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlgUI.class);
    private SqlgGraph sqlgGraph;
    public static SqlgUI INSTANCE = null;

    public static void initialize(SqlgGraph sqlgGraph, int port) {
        Preconditions.checkState(INSTANCE == null);
        set(sqlgGraph);


        Javalin javalin = Javalin.create(config -> {
            config.staticFiles.add(staticFiles -> {
                staticFiles.directory = "/home/pieter/Projects/sqlg/sqlg-ui/src/main/web/dist/dist";
                staticFiles.location = Location.EXTERNAL;
//                staticFiles.directory = "/dist";
//                staticFiles.location = Location.CLASSPATH;
            });
            config.compression.gzipOnly();
        });
        javalin.after(ctx -> SqlgUI.INSTANCE.getSqlgGraph().tx().rollback());
        JavalinResources.resources(javalin);
        javalin.ws("/sqlg/data/v1/websocket", ws -> {
            ws.onConnect(ctx -> NotificationManager.INSTANCE.add(ctx.session));
            ws.onClose(ctx -> NotificationManager.INSTANCE.remove(ctx.session));
        });
        javalin.wsAfter(wsConfig -> SqlgUI.INSTANCE.getSqlgGraph().tx().rollback());
        javalin.start(port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutting down Sqlg UI");
            javalin.stop();
            sqlgGraph.close();
        }));

        try (InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("banner")) {
            String banner = new BufferedReader(
                    new InputStreamReader(Objects.requireNonNull(inputStream), StandardCharsets.UTF_8))
                    .lines()
                    .collect(Collectors.joining("\n"));

            LOGGER.info("\n" + banner);
            LOGGER.info("Sqlg UI is available at http://localhost:{}/sqlg/v1/", port);
        } catch (IOException e) {
            //swallow
            LOGGER.info("Sqlg UI is available at http://localhost:{}/sqlg/v1/", port);
        }
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
