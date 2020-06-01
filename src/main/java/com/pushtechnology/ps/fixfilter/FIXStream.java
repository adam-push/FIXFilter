package com.pushtechnology.ps.fixfilter;

import com.pushtechnology.diffusion.client.features.Topics;
import com.pushtechnology.diffusion.client.topics.details.TopicSpecification;

import java.util.*;
import java.util.stream.Collectors;

public class FIXStream extends Topics.ValueStream.Default<String> {

    private final String topicSource;
    private final String fieldSeparator;
    private final String pairSeparator;
    private final Set<String> keySet;
    private final Queue<UpdateItem> updateQueue;

    private FIXStream() {
        topicSource = null;
        fieldSeparator = null;
        pairSeparator = null;
        keySet = null;
        updateQueue = null;
    }

    public FIXStream(String topicSource, String fieldSeparator, String pairSeparator, List<String> fixFields, Queue<UpdateItem> updateQueue) {
        this.topicSource = topicSource;

        this.fieldSeparator = fieldSeparator;
        this.pairSeparator = pairSeparator;

        this.keySet = new HashSet<>();
        this.keySet.addAll(fixFields);

        this.updateQueue = updateQueue;
    }

    @Override
    public void onValue(String topicPath, TopicSpecification specification, String oldValue, String newValue) {
        Map<String, String> extractedFields = Arrays.stream(newValue.split(fieldSeparator))
                .map(field -> field.split(pairSeparator))
                .filter(pair -> keySet.contains(pair[0]))
                .collect(Collectors.toMap(pair -> pair[0], pair -> pair[1]));

        updateQueue.add(new UpdateItem(topicPath.substring(topicSource.length() + 1), extractedFields, newValue));
    }
}
