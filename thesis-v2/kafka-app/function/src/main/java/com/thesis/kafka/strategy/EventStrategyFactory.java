package com.thesis.kafka.strategy;

import java.util.Map;

public final class EventStrategyFactory {

    private static final Map<String, EventStrategy> BY_EVENT_TYPE = Map.of(
        "RAINFALL",    new RainfallStrategy(),
        "TEMPERATURE", new TemperatureStrategy(),
        "WIND",        new WindStrategy()
    );

    private static final Map<String, EventStrategy> BY_TOPIC = Map.of(
        "weather-rain", BY_EVENT_TYPE.get("RAINFALL"),
        "weather-temp", BY_EVENT_TYPE.get("TEMPERATURE"),
        "weather-wind", BY_EVENT_TYPE.get("WIND")
    );

    private EventStrategyFactory() {
    }

    public static EventStrategy byTopic(String topic) {
        if (topic == null) {
            throw new IllegalArgumentException("topic must not be null");
        }
        EventStrategy strategy = BY_TOPIC.get(topic);
        if (strategy == null) {
            throw new IllegalArgumentException(
                "Unknown topic: " + topic + ". Supported: " + BY_TOPIC.keySet());
        }
        return strategy;
    }

    public static EventStrategy byEventType(String eventType) {
        if (eventType == null) {
            throw new IllegalArgumentException("eventType must not be null");
        }
        EventStrategy strategy = BY_EVENT_TYPE.get(eventType.toUpperCase());
        if (strategy == null) {
            throw new IllegalArgumentException(
                "Unknown event type: " + eventType + ". Supported: " + BY_EVENT_TYPE.keySet());
        }
        return strategy;
    }

    public static java.util.Set<String> supportedEventTypes() {
        return BY_EVENT_TYPE.keySet();
    }

    public static java.util.Set<String> supportedTopics() {
        return BY_TOPIC.keySet();
    }
}
