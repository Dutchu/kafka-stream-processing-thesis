package com.thesis.kafka.strategy;

import com.thesis.kafka.model.WeatherEvent;

public interface EventStrategy {

    String topicName();

    String eventType();

    WeatherEvent generate(String zoneId, double intensity);
}
