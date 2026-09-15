package com.thesis.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"status", "runId", "invocationId", "topic", "startEpochMs", "endEpochMs",
        "sent", "acked", "failed", "errors", "bytes", "avgRecordBytes", "ack",
        "achievedRatePerSec", "instanceId"})
public class RunSummary {

    @JsonProperty("status")
    private String status;

    @JsonProperty("runId")
    private String runId;

    @JsonProperty("invocationId")
    private long invocationId;

    @JsonProperty("topic")
    private String topic;

    @JsonProperty("startEpochMs")
    private long startEpochMs;

    @JsonProperty("endEpochMs")
    private long endEpochMs;

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

    @JsonProperty("avgRecordBytes")
    private long avgRecordBytes;

    @JsonProperty("ack")
    private AckStats ack;

    @JsonProperty("achievedRatePerSec")
    private double achievedRatePerSec;

    @JsonProperty("instanceId")
    private String instanceId;

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getRunId() { return runId; }
    public void setRunId(String runId) { this.runId = runId; }

    public long getInvocationId() { return invocationId; }
    public void setInvocationId(long invocationId) { this.invocationId = invocationId; }

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }

    public long getStartEpochMs() { return startEpochMs; }
    public void setStartEpochMs(long startEpochMs) { this.startEpochMs = startEpochMs; }

    public long getEndEpochMs() { return endEpochMs; }
    public void setEndEpochMs(long endEpochMs) { this.endEpochMs = endEpochMs; }

    public long getSent() { return sent; }
    public void setSent(long sent) { this.sent = sent; }

    public long getAcked() { return acked; }
    public void setAcked(long acked) { this.acked = acked; }

    public long getFailed() { return failed; }
    public void setFailed(long failed) { this.failed = failed; }

    public Map<String, Long> getErrors() { return errors; }
    public void setErrors(Map<String, Long> errors) { this.errors = errors; }

    public long getBytes() { return bytes; }
    public void setBytes(long bytes) { this.bytes = bytes; }

    public long getAvgRecordBytes() { return avgRecordBytes; }
    public void setAvgRecordBytes(long avgRecordBytes) { this.avgRecordBytes = avgRecordBytes; }

    public AckStats getAck() { return ack; }
    public void setAck(AckStats ack) { this.ack = ack; }

    public double getAchievedRatePerSec() { return achievedRatePerSec; }
    public void setAchievedRatePerSec(double achievedRatePerSec) { this.achievedRatePerSec = achievedRatePerSec; }

    public String getInstanceId() { return instanceId; }
    public void setInstanceId(String instanceId) { this.instanceId = instanceId; }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"count", "meanMs", "p50Ms", "p95Ms", "p99Ms", "maxMs"})
    public static class AckStats {

        @JsonProperty("count")
        private long count;

        @JsonProperty("meanMs")
        private double meanMs;

        @JsonProperty("p50Ms")
        private double p50Ms;

        @JsonProperty("p95Ms")
        private double p95Ms;

        @JsonProperty("p99Ms")
        private double p99Ms;

        @JsonProperty("maxMs")
        private double maxMs;

        public long getCount() { return count; }
        public void setCount(long count) { this.count = count; }

        public double getMeanMs() { return meanMs; }
        public void setMeanMs(double meanMs) { this.meanMs = meanMs; }

        public double getP50Ms() { return p50Ms; }
        public void setP50Ms(double p50Ms) { this.p50Ms = p50Ms; }

        public double getP95Ms() { return p95Ms; }
        public void setP95Ms(double p95Ms) { this.p95Ms = p95Ms; }

        public double getP99Ms() { return p99Ms; }
        public void setP99Ms(double p99Ms) { this.p99Ms = p99Ms; }

        public double getMaxMs() { return maxMs; }
        public void setMaxMs(double maxMs) { this.maxMs = maxMs; }
    }
}
