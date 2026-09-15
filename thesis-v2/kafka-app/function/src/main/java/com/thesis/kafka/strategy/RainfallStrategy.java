package com.thesis.kafka.strategy;

import com.thesis.kafka.model.WeatherEvent;
import java.util.concurrent.ThreadLocalRandom;

public class RainfallStrategy implements EventStrategy {

    private static final String TOPIC = "weather-rain";
    private static final String EVENT_TYPE = "RAINFALL";
    private static final double MAX_RAINFALL_MM = 150.0;

    @Override
    public String topicName() {
        return TOPIC;
    }

    @Override
    public String eventType() {
        return EVENT_TYPE;
    }

    @Override
    public WeatherEvent generate(String zoneId, double intensity) {
        double base = MAX_RAINFALL_MM * intensity;
        double jitter = ThreadLocalRandom.current().nextGaussian() * (base * 0.15);
        double rainfall = Math.max(0, base + jitter);

        return new WeatherEvent(zoneId, EVENT_TYPE, rainfall, "mm/h", intensity);
    }
}
