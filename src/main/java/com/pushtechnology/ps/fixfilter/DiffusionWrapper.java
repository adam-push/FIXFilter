package com.pushtechnology.ps.fixfilter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pushtechnology.diffusion.client.Diffusion;
import com.pushtechnology.diffusion.client.features.TopicUpdate;
import com.pushtechnology.diffusion.client.session.Session;
import com.pushtechnology.diffusion.client.session.SessionEstablishmentException;
import com.pushtechnology.diffusion.client.topics.details.TopicSpecification;
import com.pushtechnology.diffusion.client.topics.details.TopicType;
import com.pushtechnology.diffusion.datatype.json.JSON;
import com.pushtechnology.diffusion.datatype.json.JSONDataType;

import java.util.stream.Collectors;

public class DiffusionWrapper {
    private final Config config;
    private final ObjectMapper mapper;

    private Session session = null;
    private final TopicSpecification stringTopicSpec = Diffusion.newTopicSpecification(TopicType.STRING)
            .withProperty(TopicSpecification.CONFLATION, "none");
    private final TopicSpecification jsonTopicSpec = Diffusion.newTopicSpecification(TopicType.JSON)
            .withProperty(TopicSpecification.CONFLATION, "none");

    public DiffusionWrapper(Config config) {
        this.config = config;

        this.mapper = new ObjectMapper();
    }

    public Session getSession() {
        return session;
    }

    private Session simpleConnectionAttempt() {
        if (session != null && !session.getState().isClosed()) {
            return session;
        }

        System.out.println("sessionConnectionAttempt()");
        try {
            session = Diffusion.sessions()
                    .inputBufferSize(1024)
                    .serverHost(config.getDiffusionHost())
                    .serverPort(config.getDiffusionPort())
                    .principal(config.getDiffusionUsername())
                    .password(config.getDiffusionPassword())
                    .listener(new Session.Listener.Default() {
                        @Override
                        public void onSessionStateChanged(Session session, Session.State oldState, Session.State newState) {
                            System.out.println("Session state changed from " + oldState + " to " + newState);
                        }
                    })
                    .open();
        } catch (SessionEstablishmentException ex) {
            System.err.println("Unable to connect to Diffusion, retrying");
        }

        return session;
    }

    public void connect() {
        while (session == null || session.getState().isClosed()) {
            session = simpleConnectionAttempt();
            if (session == null) {
                try {
                    Thread.sleep(config.getDiffusionRetryTime());
                } catch (InterruptedException ignore) {
                }
                continue;
            }
        }
        System.out.println("Connected to Diffusion, session id = " + session.getSessionId());
    }

    public void close() {
        if (session != null && !session.getState().isClosed()) {
            session.close();
        }
    }

    public boolean isConnected() {
        if (session != null) {
            return session.getState().isConnected();
        }
        return false;
    }

    public void updateTopicAsJSON(UpdateItem item) {
        String path = String.join("/", config.getTopicTarget(), item.getTopic());
        path = String.join("/", path, item.getKeyPairs().values().stream().collect(Collectors.joining("/")));

        ObjectNode root = mapper.createObjectNode();
        ObjectNode keys = root.putObject("keys");
        item.getKeyPairs().forEach((k, v) -> {
            keys.put(k, v);
        });
        root.put("payload", item.getPayload());

        String data;

        try {
            data = mapper.writeValueAsString(root);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return;
        }

        getSession().feature(TopicUpdate.class).addAndSet(path, jsonTopicSpec, JSON.class, Diffusion.dataTypes().json().fromJsonString(data));
    }

    public void updateTopic(UpdateItem item) {
        String path = String.join("/", config.getTopicTarget(), item.getTopic());
        path = String.join("/", path, item.getKeyPairs().values().stream().collect(Collectors.joining("/")));

        getSession().feature(TopicUpdate.class).addAndSet(path, stringTopicSpec, String.class, item.getPayload());
    }
}
