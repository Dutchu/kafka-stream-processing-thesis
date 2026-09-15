package com.thesis.dashboard.run;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.thesis.dashboard.math.Profile;

import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.ALWAYS)
public class RunManifest {
    public String runId;
    public String exp;
    public String label;
    public String config;
    public int parallelism;
    public double ratePerSec;
    public Integer durationSec;
    public Long count;
    public List<String> topics = new ArrayList<>();
    public double intensity;
    public int runIndex;

    public long startEpochMs;
    public long stopEpochMs;
    public long analysisStartMs;

    public String functionUrl;

    public Profile profileAtStart;

    public List<Invocation> invocations = new ArrayList<>();

    public String status = "running";

    public RunSummary summary;

    public static class Invocation {
        public int invocationId;
        public String zoneId;
        public String topic;
        public boolean ok;
        public int httpStatus;
        public String error;
        public Long startEpochMs;
        public Long endEpochMs;
        public Long sent;
        public Long acked;
        public Long failed;
        public Double achievedRatePerSec;
        public String instanceId;
    }
}
