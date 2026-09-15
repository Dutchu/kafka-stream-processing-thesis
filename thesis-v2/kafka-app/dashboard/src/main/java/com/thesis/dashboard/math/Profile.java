package com.thesis.dashboard.math;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Profile {

    @JsonProperty("config")
    private String config;

    @JsonProperty("muMsgs")
    private double muMsgs = Double.NaN;

    @JsonProperty("tauAckMs")
    private double tauAckMs = Double.NaN;

    @JsonProperty("tauE2eMs")
    private double tauE2eMs = Double.NaN;

    @JsonProperty("ca2")
    private double ca2 = Double.NaN;

    @JsonProperty("cs2")
    private double cs2 = Double.NaN;

    @JsonProperty("source")
    private String source = "DEFAULT";

    public Profile() {
    }

    public Profile(String config, double muMsgs, double tauAckMs, double tauE2eMs,
                    double ca2, double cs2, String source) {
        this.config = config;
        this.muMsgs = muMsgs;
        this.tauAckMs = tauAckMs;
        this.tauE2eMs = tauE2eMs;
        this.ca2 = ca2;
        this.cs2 = cs2;
        this.source = source;
    }

    public static Profile defaultProfile(String config) {
        return new Profile(config, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, "DEFAULT");
    }

    public String getConfig() {
        return config;
    }

    public void setConfig(String config) {
        this.config = config;
    }

    public double getMuMsgs() {
        return muMsgs;
    }

    public void setMuMsgs(double muMsgs) {
        this.muMsgs = muMsgs;
    }

    public double getTauAckMs() {
        return tauAckMs;
    }

    public void setTauAckMs(double tauAckMs) {
        this.tauAckMs = tauAckMs;
    }

    public double getTauE2eMs() {
        return tauE2eMs;
    }

    public void setTauE2eMs(double tauE2eMs) {
        this.tauE2eMs = tauE2eMs;
    }

    public double getCa2() {
        return ca2;
    }

    public void setCa2(double ca2) {
        this.ca2 = ca2;
    }

    public double getCs2() {
        return cs2;
    }

    public void setCs2(double cs2) {
        this.cs2 = cs2;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }
}
