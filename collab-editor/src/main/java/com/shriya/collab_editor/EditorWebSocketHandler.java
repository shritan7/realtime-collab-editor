package com.shriya.collab_editor;

import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EditorWebSocketHandler extends TextWebSocketHandler {

    private final Set<WebSocketSession> sessions = new CopyOnWriteArraySet<>();
    private final Map<String, String> userNames = new ConcurrentHashMap<>();
    private final AtomicInteger userCounter = new AtomicInteger(1);

    // The server's single source of truth for the document
    private final StringBuilder documentState = new StringBuilder();
    private final Object docLock = new Object();

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        sessions.add(session);
        String name = "User " + userCounter.getAndIncrement();
        userNames.put(session.getId(), name);
        System.out.println(name + " connected: " + session.getId());

        // Send the new client the CURRENT document, so it starts in sync
        String currentDoc;
        synchronized (docLock) {
            currentDoc = documentState.toString();
        }
        session.sendMessage(new TextMessage("{\"type\":\"sync\",\"text\":" + toJsonString(currentDoc) + "}"));

        broadcastPresence();
    }

    @Override
    protected void handleTextMessage(WebSocketSession senderSession, TextMessage message) throws Exception {
        String payload = message.getPayload();

        // Keep the server's copy of the document up to date too
        Operation op = parseOperation(payload);
        if (op != null) {
            synchronized (docLock) {
                try {
                    documentState.replace(op.position(), op.position() + op.deleteCount(), op.insertText());
                } catch (Exception e) {
                    System.out.println("Skipped an out-of-sync operation: " + e.getMessage());
                }
            }
        }

        // Relay to everyone else, same as before
        for (WebSocketSession session : sessions) {
            if (session.isOpen() && !session.getId().equals(senderSession.getId())) {
                session.sendMessage(message);
            }
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessions.remove(session);
        userNames.remove(session.getId());
        broadcastPresence();
    }

    private void broadcastPresence() {
        String usersJson = userNames.values().stream()
                .map(name -> "\"" + name + "\"")
                .reduce((a, b) -> a + "," + b)
                .orElse("");
        String payload = "{\"type\":\"presence\",\"users\":[" + usersJson + "]}";
        for (WebSocketSession session : sessions) {
            try {
                if (session.isOpen()) session.sendMessage(new TextMessage(payload));
            } catch (Exception e) {
                System.out.println("Failed to send presence update: " + e.getMessage());
            }
        }
    }

    // --- Small hand-written JSON helpers (no external library needed) ---

    private record Operation(int position, int deleteCount, String insertText) {}

    private Operation parseOperation(String json) {
        try {
            int position = extractInt(json, "position");
            int deleteCount = extractInt(json, "deleteCount");
            String insertText = extractString(json, "insertText");
            return new Operation(position, deleteCount, insertText);
        } catch (Exception e) {
            return null;
        }
    }

    private int extractInt(String json, String key) {
        Matcher m = Pattern.compile("\"" + key + "\":(-?\\d+)").matcher(json);
        if (m.find()) return Integer.parseInt(m.group(1));
        throw new IllegalArgumentException("missing " + key);
    }

    private String extractString(String json, String key) {
        Matcher m = Pattern.compile("\"" + key + "\":\"((?:[^\"\\\\]|\\\\.)*)\"").matcher(json);
        if (m.find()) return unescape(m.group(1));
        throw new IllegalArgumentException("missing " + key);
    }

    private String unescape(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char next = s.charAt(++i);
                switch (next) {
                    case 'n' -> sb.append('\n');
                    case 't' -> sb.append('\t');
                    case 'r' -> sb.append('\r');
                    case '"' -> sb.append('"');
                    case '\\' -> sb.append('\\');
                    default -> sb.append(next);
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private String toJsonString(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> sb.append(c);
            }
        }
        return sb.append("\"").toString();
    }
}