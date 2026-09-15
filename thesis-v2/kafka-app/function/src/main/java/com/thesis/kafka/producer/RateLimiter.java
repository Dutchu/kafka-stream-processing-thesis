package com.thesis.kafka.producer;

import java.util.concurrent.locks.LockSupport;

public final class RateLimiter {

    private static final long PARK_THRESHOLD_NS = 2_000_000L;

    private final double ratePerSec;
    private final long intervalNanos;
    private long nextTickNanos;
    private boolean started;

    public RateLimiter(double ratePerSec) {
        this.ratePerSec = ratePerSec;
        this.intervalNanos = ratePerSec > 0 ? (long) (1_000_000_000.0 / ratePerSec) : 0L;
    }

    public boolean isLimited() {
        return ratePerSec > 0;
    }

    public void acquire() {
        if (!isLimited()) {
            return;
        }
        long now = System.nanoTime();
        if (!started) {
            nextTickNanos = now + intervalNanos;
            started = true;
            return;
        }
        long remaining = nextTickNanos - now;
        if (remaining > 0) {
            waitUntil(remaining);
        }
        nextTickNanos += intervalNanos;
        long behind = System.nanoTime() - nextTickNanos;
        if (behind > intervalNanos) {
            nextTickNanos = System.nanoTime() + intervalNanos;
        }
    }

    private void waitUntil(long remainingNanos) {
        long remaining = remainingNanos;
        while (remaining > PARK_THRESHOLD_NS) {
            LockSupport.parkNanos(remaining - PARK_THRESHOLD_NS);
            remaining = nextTickNanos - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
        }
        while (System.nanoTime() < nextTickNanos) {
            Thread.onSpinWait();
        }
    }
}
