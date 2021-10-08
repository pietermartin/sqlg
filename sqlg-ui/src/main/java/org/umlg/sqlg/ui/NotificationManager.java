package org.umlg.sqlg.ui;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.eclipse.jetty.websocket.api.Session;
import org.umlg.sqlg.ui.util.ObjectMapperFactory;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class NotificationManager {

    private final Queue<Session> sessions = new ConcurrentLinkedQueue<>();
    public static NotificationManager INSTANCE = new NotificationManager();

    private enum MessageType {
        NOTIFICATION,
        UI
    }

    public enum Severity {
        INFO,
        SUCCESS,
        FAILURE
    }

    private NotificationManager() {
    }

    public void add(Session session) {
        this.sessions.add(session);
    }

    public void remove(Session session) {
        this.sessions.remove(session);
    }

    public void sendRefreshVertexLabels(String schemaName, String message) {
        ObjectMapper objectMapper = ObjectMapperFactory.INSTANCE.getObjectMapper();
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("type", MessageType.UI.name());
        objectNode.put("subType", "refreshVertexLabels");
        objectNode.put("schemaName", schemaName);
        objectNode.put("message", message);
        send(objectNode.toString());
    }

    public void sendRefreshEdgeLabels(String schemaName, String message) {
        ObjectMapper objectMapper = ObjectMapperFactory.INSTANCE.getObjectMapper();
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("type", MessageType.UI.name());
        objectNode.put("subType", "refreshEdgeLabels");
        objectNode.put("schemaName", schemaName);
        objectNode.put("message", message);
        send(objectNode.toString());
    }

    public void sendRefreshTree(String selectedTreeId) {
        ObjectMapper objectMapper = ObjectMapperFactory.INSTANCE.getObjectMapper();
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("type", MessageType.UI.name());
        objectNode.put("subType", "refreshTree");
        objectNode.put("selectedTreeId", selectedTreeId);
        send(objectNode.toString());
    }

    public void sendNotification(String message) {
        ObjectMapper objectMapper = ObjectMapperFactory.INSTANCE.getObjectMapper();
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("type", MessageType.NOTIFICATION.name());
        objectNode.put("message", message);
        send(objectNode.toString());
    }

    public void sendRefreshAbstractLabel(String schema, String abstractLabel, String vertexOrEdge, String message) {
        ObjectMapper objectMapper = ObjectMapperFactory.INSTANCE.getObjectMapper();
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("type", MessageType.UI.name());
        objectNode.put("schema", schema);
        objectNode.put("abstractLabel", abstractLabel);
        objectNode.put("vertexOrEdge", vertexOrEdge);
        objectNode.put("subType", "abstractLabel");
        objectNode.put("message", message);
        objectNode.put("severity", "success");
        send(objectNode.toString());
    }

    private void send(String message) {
        for (Session session : this.sessions) {
            try {
                session.getRemote().sendString(message); // and send it back
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
