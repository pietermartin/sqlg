package org.umlg.sqlg.ui;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;

import java.io.IOException;

@WebSocket
public class SqlgWebsocket {

    @OnWebSocketConnect
    public void connected(Session session) {
        NotificationManager.INSTANCE.add(session);
    }

    @OnWebSocketClose
    public void closed(Session session, int statusCode, String reason) {
        NotificationManager.INSTANCE.remove(session);
    }

    @OnWebSocketMessage
    public void message(Session session, String message) throws IOException {
//        System.out.println("Got: " + message);   // Print message
    }

}
