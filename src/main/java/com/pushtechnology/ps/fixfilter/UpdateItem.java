package com.pushtechnology.ps.fixfilter;

import java.util.Map;

public class UpdateItem {
    private final String topic;
    private final Map<String, String> keyPairs;
    private final String payload;

    public UpdateItem(String topic, Map<String, String> keyPairs, String payload) {
        this.topic = topic;
        this.keyPairs = keyPairs;
        this.payload = payload;
    }

    public String getTopic() {
        return topic;
    }

    public Map<String, String> getKeyPairs() {
        return keyPairs;
    }

    public String getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "UpdateItem{" +
                "topic='" + topic + '\'' +
                ", keyPairs=" + keyPairs +
                ", payload='" + payload + '\'' +
                '}';
    }
}
