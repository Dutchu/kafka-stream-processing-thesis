package com.thesis.dashboard.run;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;

@JsonInclude(JsonInclude.Include.ALWAYS)
public class RunSummary {
    public double lambdaMean = Double.NaN;
    public double lambdaP50 = Double.NaN;
    public double ackMean = Double.NaN;
    public double ackP50 = Double.NaN;
    public double ackP95 = Double.NaN;
    public double ackP99 = Double.NaN;
    public double ackMax = Double.NaN;
    public double ackSdMs = Double.NaN;
    public double e2eMean = Double.NaN;
    public double e2eP50 = Double.NaN;
    public double e2eP99 = Double.NaN;
    public double rhoMean = Double.NaN;
    public double predAck = Double.NaN;
    public double predE2e = Double.NaN;
    public double errAck = Double.NaN;
    public double errE2e = Double.NaN;
    public long consumerLagMax = 0;
    public boolean consumerLimited = false;
    public long sent = 0;
    public long acked = 0;
    public long failed = 0;
    public Map<String, Long> errors = Map.of();
    public double avgRecordBytes = Double.NaN;
    public SaturationSignature saturationSignature = null;

    public static class SaturationSignature {
        public Double handlerIdle;
        public Double netIdle;
        public Double cpuBusy;
        public Double requestQueue;
        public boolean fired;
    }
}
