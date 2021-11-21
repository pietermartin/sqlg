package org.umlg.sqlg.gremlin.server;

import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;

import java.io.InputStream;

class SqlgGremlinServer {

    private GremlinServer server;

    void start(InputStream inputStream) throws Exception {
        Settings settings = Settings.read(inputStream);
        this.server = new GremlinServer(settings);
        this.server.start().join();
    }

    void stop() {
        this.server.stop().join();
    }
}
