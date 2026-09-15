package com.thesis.kafka.strategy;

import com.thesis.kafka.model.WeatherEvent;
import java.util.concurrent.ThreadLocalRandom;

public class WindStrategy implements EventStrategy {

    private static final String TOPIC = "weather-wind";
    private static final String EVENT_TYPE = "WIND";
    private static final double MAX_WIND_KMH = 200.0;

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
        double base = MAX_WIND_KMH * intensity;
        double jitter = ThreadLocalRandom.current().nextGaussian() * (base * 0.2);
        double wind = Math.max(0, base + jitter);

        return new WeatherEvent(zoneId, EVENT_TYPE, wind, "km/h", intensity);
    }
}
