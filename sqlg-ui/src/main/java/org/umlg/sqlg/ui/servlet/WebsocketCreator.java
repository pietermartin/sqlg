package org.umlg.sqlg.ui.servlet;

import org.eclipse.jetty.websocket.server.JettyServerUpgradeRequest;
import org.eclipse.jetty.websocket.server.JettyServerUpgradeResponse;
import org.eclipse.jetty.websocket.server.JettyWebSocketCreator;

public class WebsocketCreator implements JettyWebSocketCreator {

    @Override
    public Object createWebSocket(JettyServerUpgradeRequest req, JettyServerUpgradeResponse resp) {
        try {
            return new SqlgServletWebsocket();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
