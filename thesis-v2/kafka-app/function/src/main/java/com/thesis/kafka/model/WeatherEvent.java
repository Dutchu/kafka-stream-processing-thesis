package com.thesis.kafka.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class WeatherEvent {

    @JsonProperty("zoneId")
    private String zoneId;

    @JsonProperty("eventType")
    private String eventType;

    @JsonProperty("value")
    private double value;

    @JsonProperty("unit")
    private String unit;

    @JsonProperty("intensity")
    private double intensity;

    @JsonProperty("timestamp")
    private long timestamp;

    public WeatherEvent() {
    }

    public WeatherEvent(String zoneId, String eventType, double value,
                        String unit, double intensity) {
        this.zoneId = zoneId;
        this.eventType = eventType;
        this.value = value;
        this.unit = unit;
        this.intensity = intensity;
        this.timestamp = System.currentTimeMillis();
    }

    public String getZoneId()    { return zoneId; }
    public String getEventType() { return eventType; }
    public double getValue()     { return value; }
    public String getUnit()      { return unit; }
    public double getIntensity() { return intensity; }
    public long getTimestamp()    { return timestamp; }

    public void setZoneId(String zoneId)       { this.zoneId = zoneId; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    public void setValue(double value)          { this.value = value; }
    public void setUnit(String unit)            { this.unit = unit; }
    public void setIntensity(double intensity)  { this.intensity = intensity; }
    public void setTimestamp(long timestamp)    { this.timestamp = timestamp; }
}
