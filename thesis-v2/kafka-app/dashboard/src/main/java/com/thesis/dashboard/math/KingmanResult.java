package com.thesis.dashboard.math;

public record KingmanResult(
        double lambda,
        double mu,
        double rho,
        double rhoEff,
        boolean overload,
        double wqMs,
        double predAckMs,
        double predE2eMs,
        double obsAckMeanMs,
        double obsE2eMeanMs,
        double errAckMs,
        double errE2eMs,
        double errAckRel,
        double errE2eRel,
        double maeAck60s,
        double maeE2e60s,
        double ca2,
        double cs2,
        double ca2Est,
        double cs2Est
) {
    public static KingmanResult nan() {
        double n = Double.NaN;
        return new KingmanResult(n, n, n, n, false, n, n, n, n, n, n, n, n, n, n, n, n, n, n, n);
    }
}
