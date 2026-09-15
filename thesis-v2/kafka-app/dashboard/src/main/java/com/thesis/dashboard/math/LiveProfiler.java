package com.thesis.dashboard.math;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

public class LiveProfiler {

    private final int window;
    private final Deque<Double> lambdas = new ArrayDeque<>();
    private final Deque<Double> ackP50s = new ArrayDeque<>();

    public LiveProfiler() {
        this(60);
    }

    public LiveProfiler(int windowSeconds) {
        this.window = Math.max(10, windowSeconds);
    }

    private final Deque<Double> ca2s = new ArrayDeque<>();
    private final Deque<Double> cs2s = new ArrayDeque<>();
    private final Deque<Double> e2es = new ArrayDeque<>();

    public synchronized void observe(double lambda, double ackP50, double e2eP50,
                                     double ca2Est, double cs2Est) {
        push(lambdas, lambda);
        push(ackP50s, ackP50);
        push(e2es, e2eP50);
        push(ca2s, ca2Est);
        push(cs2s, cs2Est);
    }

    private void push(Deque<Double> q, double v) {
        if (Double.isFinite(v)) q.addLast(v);
        while (q.size() > window) q.removeFirst();
    }

    public synchronized LiveSnapshot snapshot() {
        if (lambdas.isEmpty() || ackP50s.isEmpty()) return null;
        double mu = max(lambdas);
        double tauAck = median(ackP50s);
        double tauE2e = e2es.isEmpty() ? Double.NaN : median(e2es);
        double ca2 = ca2s.isEmpty() ? 1.0 : median(ca2s);
        double cs2 = cs2s.isEmpty() ? Double.NaN : median(cs2s);
        return new LiveSnapshot(mu, tauAck, tauE2e, ca2, cs2, lambdas.size());
    }

    public record LiveSnapshot(double muMsgs, double tauAckMs, double tauE2eMs,
                               double ca2, double cs2, int n) {
    }

    private static double median(Deque<Double> q) {
        List<Double> s = new ArrayList<>(q);
        java.util.Collections.sort(s);
        int n = s.size();
        if (n == 0) return Double.NaN;
        if (n % 2 == 1) return s.get(n / 2);
        return (s.get(n / 2 - 1) + s.get(n / 2)) / 2.0;
    }

    private static double max(Deque<Double> q) {
        double m = Double.NaN;
        for (double v : q) m = Double.isNaN(m) ? v : Math.max(m, v);
        return m;
    }
}
