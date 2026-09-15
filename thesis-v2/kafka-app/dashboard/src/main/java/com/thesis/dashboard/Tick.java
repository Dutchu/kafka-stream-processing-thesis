package com.thesis.dashboard;

import com.thesis.dashboard.cluster.ClusterSnapshot;
import com.thesis.dashboard.math.Profile;

public record Tick(
        long tsMs,
        String runId,
        double lambdaLeo,
        double lambdaConsumer,
        double rho,
        double wqMs,
        double predAckMs,
        double predE2eMs,
        double ackMeanMs,
        double ackP50Ms,
        double ackP95Ms,
        double ackP99Ms,
        double ackMaxMs,
        long ackCount,
        long ackInvocations,
        double e2eMeanMs,
        double e2eP50Ms,
        double e2eP99Ms,
        double e2eMaxMs,
        long e2eCount,
        double errAckMs,
        double errE2eMs,
        double errAckRel,
        double errE2eRel,
        long consumerLag,
        long sent,
        long acked,
        long failed,
        String errorTypes,
        double ca2Est,
        double cs2Est,
        long totalMsgsCluster,
        double bytesRate,
        ClusterSnapshot cluster,
        RunStatus run,
        Profile profile
) {
    public record RunStatus(boolean active, String runId, String exp, long startEpochMs, long elapsedSec) {
        public static RunStatus idle() {
            return new RunStatus(false, null, null, 0, 0);
        }
    }
}
