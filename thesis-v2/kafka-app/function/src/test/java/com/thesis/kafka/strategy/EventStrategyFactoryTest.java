package com.thesis.kafka.strategy;

import com.thesis.kafka.model.WeatherEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventStrategyFactoryTest {

    @Test
    void byTopicResolvesRainfall() {
        EventStrategy strategy = EventStrategyFactory.byTopic("weather-rain");
        assertInstanceOf(RainfallStrategy.class, strategy);
        assertEquals("weather-rain", strategy.topicName());
        assertEquals("RAINFALL", strategy.eventType());
    }

    @Test
    void byTopicResolvesTemperature() {
        EventStrategy strategy = EventStrategyFactory.byTopic("weather-temp");
        assertInstanceOf(TemperatureStrategy.class, strategy);
        assertEquals("weather-temp", strategy.topicName());
        assertEquals("TEMPERATURE", strategy.eventType());
    }

    @Test
    void byTopicResolvesWind() {
        EventStrategy strategy = EventStrategyFactory.byTopic("weather-wind");
        assertInstanceOf(WindStrategy.class, strategy);
        assertEquals("weather-wind", strategy.topicName());
        assertEquals("WIND", strategy.eventType());
    }

    @Test
    void byEventTypeResolvesAllThreeCaseInsensitively() {
        assertInstanceOf(RainfallStrategy.class, EventStrategyFactory.byEventType("rainfall"));
        assertInstanceOf(TemperatureStrategy.class, EventStrategyFactory.byEventType("Temperature"));
        assertInstanceOf(WindStrategy.class, EventStrategyFactory.byEventType("WIND"));
    }

    @Test
    void byTopicRejectsUnknownTopic() {
        assertThrows(IllegalArgumentException.class, () -> EventStrategyFactory.byTopic("weather-seismic"));
        assertThrows(IllegalArgumentException.class, () -> EventStrategyFactory.byTopic("unknown-topic"));
    }

    @Test
    void byEventTypeRejectsSeismic() {
        assertThrows(IllegalArgumentException.class, () -> EventStrategyFactory.byEventType("SEISMIC"));
    }

    @Test
    void byTopicRejectsNull() {
        assertThrows(IllegalArgumentException.class, () -> EventStrategyFactory.byTopic(null));
    }

    @Test
    void byEventTypeRejectsNull() {
        assertThrows(IllegalArgumentException.class, () -> EventStrategyFactory.byEventType(null));
    }

    @Test
    void supportedSetsContainExactlyThreeEntriesEach() {
        assertEquals(3, EventStrategyFactory.supportedTopics().size());
        assertEquals(3, EventStrategyFactory.supportedEventTypes().size());
        assertTrue(EventStrategyFactory.supportedTopics().containsAll(
                java.util.List.of("weather-rain", "weather-temp", "weather-wind")));
        assertTrue(EventStrategyFactory.supportedEventTypes().containsAll(
                java.util.List.of("RAINFALL", "TEMPERATURE", "WIND")));
    }

    @Test
    void generatedEventCarriesZoneIdAndEventTypeAndPositiveTimestamp() {
        EventStrategy strategy = EventStrategyFactory.byTopic("weather-rain");
        WeatherEvent event = strategy.generate("zone-007", 0.9);
        assertEquals("zone-007", event.getZoneId());
        assertEquals("RAINFALL", event.getEventType());
        assertEquals("mm/h", event.getUnit());
        assertTrue(event.getTimestamp() > 0);
        assertTrue(event.getValue() >= 0.0);
    }
}
