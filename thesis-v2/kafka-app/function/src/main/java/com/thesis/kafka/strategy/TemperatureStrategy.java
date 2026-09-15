package com.thesis.kafka.strategy;

import com.thesis.kafka.model.WeatherEvent;
import java.util.concurrent.ThreadLocalRandom;

public class TemperatureStrategy implements EventStrategy {

    private static final String TOPIC = "weather-temp";
    private static final String EVENT_TYPE = "TEMPERATURE";

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
        double baseTemp = 20.0 + (intensity * 25.0);
        double jitter = ThreadLocalRandom.current().nextGaussian() * 3.0;
        double temperature = Math.clamp(baseTemp + jitter, -20.0, 50.0);

        return new WeatherEvent(zoneId, EVENT_TYPE, temperature, "°C", intensity);
    }
}
