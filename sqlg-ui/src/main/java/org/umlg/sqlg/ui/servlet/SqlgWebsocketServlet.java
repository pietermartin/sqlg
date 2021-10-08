package org.umlg.sqlg.ui.servlet;

import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlgWebsocketServlet extends WebSocketServlet {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlgWebsocketServlet.class.getPackage().getName());

    @Override
    public void configure(WebSocketServletFactory factory) {
        LOGGER.debug("SqlgWebsocketServlet start");
        factory.getPolicy().setIdleTimeout(60000);
        factory.setCreator(new WebsocketCreator());
    }
}
