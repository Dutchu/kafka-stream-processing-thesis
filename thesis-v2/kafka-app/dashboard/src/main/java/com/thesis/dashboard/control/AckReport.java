package com.thesis.dashboard.control;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AckReport {

    @JsonProperty("runId")
    private String runId;

    @JsonProperty("invocationId")
    private int invocationId;

    @JsonProperty("topic")
    private String topic;

    @JsonProperty("windowStartMs")
    private long windowStartMs;

    @JsonProperty("windowEndMs")
    private long windowEndMs;

    @JsonProperty("sent")
    private long sent;

    @JsonProperty("acked")
    private long acked;

    @JsonProperty("failed")
    private long failed;

    @JsonProperty("errors")
    private Map<String, Long> errors;

    @JsonProperty("bytes")
    private long bytes;

    @JsonProperty("ackMeanMs")
    private double ackMeanMs;

    @JsonProperty("ackP50Ms")
    private double ackP50Ms;

    @JsonProperty("ackP99Ms")
    private double ackP99Ms;

    @JsonProperty("ackMaxMs")
    private double ackMaxMs;

    @JsonProperty("hdrBase64")
    private String hdrBase64;

    @JsonProperty("final")
    private boolean isFinal;

    public AckReport() {
    }

    public String getRunId() {
        return runId;
    }

    public void setRunId(String runId) {
        this.runId = runId;
    }

    public int getInvocationId() {
        return invocationId;
    }

    public void setInvocationId(int invocationId) {
        this.invocationId = invocationId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getWindowStartMs() {
        return windowStartMs;
    }

    public void setWindowStartMs(long windowStartMs) {
        this.windowStartMs = windowStartMs;
    }

    public long getWindowEndMs() {
        return windowEndMs;
    }

    public void setWindowEndMs(long windowEndMs) {
        this.windowEndMs = windowEndMs;
    }

    public long getSent() {
        return sent;
    }

    public void setSent(long sent) {
        this.sent = sent;
    }

    public long getAcked() {
        return acked;
    }

    public void setAcked(long acked) {
        this.acked = acked;
    }

    public long getFailed() {
        return failed;
    }

    public void setFailed(long failed) {
        this.failed = failed;
    }

    public Map<String, Long> getErrors() {
        return errors;
    }

    public void setErrors(Map<String, Long> errors) {
        this.errors = errors;
    }

    public long getBytes() {
        return bytes;
    }

    public void setBytes(long bytes) {
        this.bytes = bytes;
    }

    public double getAckMeanMs() {
        return ackMeanMs;
    }

    public void setAckMeanMs(double ackMeanMs) {
        this.ackMeanMs = ackMeanMs;
    }

    public double getAckP50Ms() {
        return ackP50Ms;
    }

    public void setAckP50Ms(double ackP50Ms) {
        this.ackP50Ms = ackP50Ms;
    }

    public double getAckP99Ms() {
        return ackP99Ms;
    }

    public void setAckP99Ms(double ackP99Ms) {
        this.ackP99Ms = ackP99Ms;
    }

    public double getAckMaxMs() {
        return ackMaxMs;
    }

    public void setAckMaxMs(double ackMaxMs) {
        this.ackMaxMs = ackMaxMs;
    }

    public String getHdrBase64() {
        return hdrBase64;
    }

    public void setHdrBase64(String hdrBase64) {
        this.hdrBase64 = hdrBase64;
    }

    @JsonProperty("final")
    public boolean isFinal() {
        return isFinal;
    }

    @JsonProperty("final")
    public void setFinal(boolean aFinal) {
        isFinal = aFinal;
    }
}
