package com.thesis.dashboard.export;

import com.thesis.dashboard.Tick;
import com.thesis.dashboard.cluster.ClusterSnapshot;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class PrometheusExporter {

    private volatile Tick lastTick;
    private volatile boolean runActive = false;
    private volatile String runIdLabel = "";
    private volatile String expLabel = "";

    private final AtomicLong ackSentTotal = new AtomicLong(0);
    private final AtomicLong ackFailedTotal = new AtomicLong(0);

    public void update(Tick tick) {
        this.lastTick = tick;
        if (tick.sent() > 0) ackSentTotal.addAndGet(0);
    }

    public void addSent(long delta) {
        if (delta > 0) ackSentTotal.addAndGet(delta);
    }

    public void addFailed(long delta) {
        if (delta > 0) ackFailedTotal.addAndGet(delta);
    }

    public void setRunActive(boolean active, String runId, String exp) {
        this.runActive = active;
        this.runIdLabel = runId == null ? "" : runId;
        this.expLabel = exp == null ? "" : exp;
    }

    public String render() {
        Tick t = lastTick;
        StringBuilder sb = new StringBuilder();
        if (t == null) {
            appendRunGauge(sb, false, "", "");
            return sb.toString();
        }

        gauge(sb, "thesis_kingman_lambda_msgs", t.lambdaLeo());
        gauge(sb, "thesis_kingman_mu_msgs", t.profile() != null ? t.profile().getMuMsgs() : Double.NaN);
        gauge(sb, "thesis_kingman_rho", t.rho());
        gauge(sb, "thesis_kingman_wq_ms", t.wqMs());
        gauge(sb, "thesis_kingman_pred_ack_ms", t.predAckMs());
        gauge(sb, "thesis_kingman_pred_e2e_ms", t.predE2eMs());
        gauge(sb, "thesis_kingman_obs_ack_ms", t.ackMeanMs());
        gauge(sb, "thesis_kingman_obs_e2e_ms", t.e2eMeanMs());
        gauge(sb, "thesis_kingman_err_ack_ms", t.errAckMs());
        gauge(sb, "thesis_kingman_err_e2e_ms", t.errE2eMs());
        gauge(sb, "thesis_kingman_err_ack_rel", t.errAckRel());
        gauge(sb, "thesis_kingman_err_e2e_rel", t.errE2eRel());
        gauge(sb, "thesis_kingman_ca2", t.profile() != null ? t.profile().getCa2() : Double.NaN);
        gauge(sb, "thesis_kingman_cs2", t.profile() != null ? t.profile().getCs2() : Double.NaN);
        gauge(sb, "thesis_kingman_ca2_est", t.ca2Est());
        gauge(sb, "thesis_kingman_cs2_est", t.cs2Est());
        gauge(sb, "thesis_ack_p50_ms", t.ackP50Ms());
        gauge(sb, "thesis_ack_p99_ms", t.ackP99Ms());
        counter(sb, "thesis_ack_sent_total", ackSentTotal.get());
        counter(sb, "thesis_ack_failed_total", ackFailedTotal.get());
        gauge(sb, "thesis_e2e_p50_ms", t.e2eP50Ms());
        gauge(sb, "thesis_e2e_p99_ms", t.e2eP99Ms());
        gauge(sb, "thesis_consumer_rate_msgs", t.lambdaConsumer());
        gauge(sb, "thesis_consumer_lag_msgs", t.consumerLag());
        gauge(sb, "thesis_cluster_total_msgs", t.totalMsgsCluster());

        ClusterSnapshot cluster = t.cluster();
        if (cluster != null) {
            appendHelpType(sb, "thesis_broker_msgs", "gauge");
            for (ClusterSnapshot.PerBrokerAgg agg : cluster.perBroker()) {
                for (Map.Entry<String, ClusterSnapshot.TopicRole> e : agg.byTopic().entrySet()) {
                    sb.append("thesis_broker_msgs{broker=\"").append(agg.brokerId())
                      .append("\",topic=\"").append(escape(e.getKey()))
                      .append("\",role=\"leader\"} ").append(fmt(e.getValue().leaderMsgs())).append('\n');
                    sb.append("thesis_broker_msgs{broker=\"").append(agg.brokerId())
                      .append("\",topic=\"").append(escape(e.getKey()))
                      .append("\",role=\"follower\"} ").append(fmt(e.getValue().followerMsgs())).append('\n');
                }
            }
        }

        appendRunGauge(sb, runActive, runIdLabel, expLabel);

        gauge(sb, "thesis_admin_errors_total", cluster != null ? cluster.adminErrors() : 0);

        return sb.toString();
    }

    private void appendRunGauge(StringBuilder sb, boolean active, String runId, String exp) {
        appendHelpType(sb, "thesis_run_active", "gauge");
        sb.append("thesis_run_active{run_id=\"").append(escape(runId))
          .append("\",exp=\"").append(escape(exp)).append("\"} ")
          .append(active ? 1 : 0).append('\n');
    }

    private static void gauge(StringBuilder sb, String name, double value) {
        appendHelpType(sb, name, "gauge");
        sb.append(name).append(' ').append(fmt(value)).append('\n');
    }

    private static void gauge(StringBuilder sb, String name, long value) {
        gauge(sb, name, (double) value);
    }

    private static void counter(StringBuilder sb, String name, long value) {
        appendHelpType(sb, name, "counter");
        sb.append(name).append(' ').append(value).append('\n');
    }

    private static void appendHelpType(StringBuilder sb, String name, String type) {
        sb.append("# TYPE ").append(name).append(' ').append(type).append('\n');
    }

    private static String fmt(double v) {
        if (!Double.isFinite(v)) return "NaN";
        return String.format(Locale.ROOT, "%.4f", v);
    }

    private static String escape(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }
}
