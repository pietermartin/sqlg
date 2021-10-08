package org.umlg.sqlg.ui.servlet;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.umlg.sqlg.ui.NotificationManager;

//@WebSocket
public class SqlgServletWebsocket implements WebSocketListener {

    private Session outbound;

    @Override
    public void onWebSocketBinary(byte[] payload, int offset, int len) {

    }

    @Override
    public void onWebSocketText(String message) {

    }

    @Override
    public void onWebSocketClose(int statusCode, String reason) {
        NotificationManager.INSTANCE.remove(this.outbound);
        this.outbound = null;
    }

    @Override
    public void onWebSocketConnect(Session session) {
        this.outbound = session;
        NotificationManager.INSTANCE.add(this.outbound);
    }

    @Override
    public void onWebSocketError(Throwable cause) {
        NotificationManager.INSTANCE.remove(this.outbound);
        this.outbound = null;
    }

}
