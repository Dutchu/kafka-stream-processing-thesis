package com.thesis.kafka.producer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RateLimiterTest {

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void achievesConfiguredRateWithinThreePercent_100PerSec() {
        assertAchievedRateWithinTolerance(100.0, 2.0, 0.03);
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void achievesConfiguredRateWithinThreePercent_5000PerSec() {
        assertAchievedRateWithinTolerance(5000.0, 2.0, 0.03);
    }

    @Test
    void zeroRateMeansUnlimited() {
        RateLimiter limiter = new RateLimiter(0.0);
        assertFalse(limiter.isLimited());

        long start = System.nanoTime();
        for (int i = 0; i < 100_000; i++) {
            limiter.acquire();
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
        assertTrue(elapsedMs < 2000, "Unlimited limiter took too long: " + elapsedMs + " ms");
    }

    @Test
    void negativeRateAlsoTreatedAsUnlimited() {
        RateLimiter limiter = new RateLimiter(-5.0);
        assertFalse(limiter.isLimited());
    }

    @Test
    void positiveRateIsLimited() {
        RateLimiter limiter = new RateLimiter(10.0);
        assertTrue(limiter.isLimited());
    }

    private void assertAchievedRateWithinTolerance(double targetRatePerSec, double windowSeconds, double tolerance) {
        RateLimiter limiter = new RateLimiter(targetRatePerSec);
        long durationNanos = (long) (windowSeconds * 1_000_000_000L);
        long start = System.nanoTime();
        long count = 0;
        while (System.nanoTime() - start < durationNanos) {
            limiter.acquire();
            count++;
        }
        long elapsedNanos = System.nanoTime() - start;
        double achievedRate = count / (elapsedNanos / 1_000_000_000.0);

        double lowerBound = targetRatePerSec * (1 - tolerance);
        double upperBound = targetRatePerSec * (1 + tolerance);
        assertTrue(achievedRate >= lowerBound && achievedRate <= upperBound,
                String.format("Achieved rate %.2f msg/s outside +-%.0f%% of target %.2f (bounds [%.2f, %.2f])",
                        achievedRate, tolerance * 100, targetRatePerSec, lowerBound, upperBound));
    }
}
