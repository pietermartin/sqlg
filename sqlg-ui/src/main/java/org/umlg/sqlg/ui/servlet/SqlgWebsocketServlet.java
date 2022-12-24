package org.umlg.sqlg.ui.servlet;

import org.eclipse.jetty.websocket.server.JettyWebSocketServlet;
import org.eclipse.jetty.websocket.server.JettyWebSocketServletFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlgWebsocketServlet extends JettyWebSocketServlet {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlgWebsocketServlet.class.getPackage().getName());

    @Override
    public void configure(JettyWebSocketServletFactory factory) {
        LOGGER.debug("SqlgWebsocketServlet start");
//        factory.getPolicy().setIdleTimeout(60000);
        factory.setCreator(new WebsocketCreator());
    }
}
