package com.pushtechnology.ps.fixfilter;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class Config extends Properties {

    private List<String> fixFields;

    public Config(String filename) {
        try {
            load(new FileInputStream(filename));
        } catch (IOException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    public String getDiffusionHost() {
        return getProperty("DIFFUSION.host", "locahost");
    }

    public int getDiffusionPort() {
        return Integer.parseInt(getProperty("DIFFUSION.port", "8080"));
    }

    public String getDiffusionUsername() {
        return getProperty("DIFFUSION.username", "control");
    }

    public String getDiffusionPassword() {
        return getProperty("DIFFUSION.password", "password");
    }

    public long getDiffusionRetryTime() {
        return Long.parseLong(getProperty("DIFFUSION.retry_time", "2000"));
    }

    public String getTopicSource() {
        return getProperty("FIX.topic.source", "FIX");
    }

    public String getTopicTarget() {
        return getProperty("FIX.topic.target", "filtered");
    }

    public String getFieldSeparator() {
        return getProperty("FIX.field.separator", ";");
    }

    public String getPairSeparator() {
        return getProperty("FIX.pair.separator", "=");
    }

    public List<String> getFIXFields() {
        // Already calculated?
        if (fixFields != null) {
            return fixFields;
        }

        String str = getProperty("FIX.fields");
        if (str == null) {
            fixFields = Collections.EMPTY_LIST;
        } else {
            fixFields = Arrays.stream(str.split(","))
                    .filter(x -> !x.isBlank())
                    .map(x -> x.trim())
                    .collect(Collectors.toList());
        }

        return fixFields;
    }
}

