package com.thesis.dashboard.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.dashboard.Tick;
import com.thesis.dashboard.control.AckSnapshot;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class RunStore implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(RunStore.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Long>> ERRORS_TYPE = new TypeReference<>() {
    };

    static final String[] SERIES_VALUE_COLUMNS = {
            "lambda_leo", "lambda_consumer", "rho", "wq_ms", "pred_ack_ms", "pred_e2e_ms",
            "ack_mean_ms", "ack_p50_ms", "ack_p95_ms", "ack_p99_ms", "ack_max_ms",
            "ack_count", "ack_invocations",
            "e2e_mean_ms", "e2e_p50_ms", "e2e_p99_ms", "e2e_max_ms", "e2e_count",
            "err_ack_ms", "err_e2e_ms", "err_ack_rel", "err_e2e_rel",
            "consumer_lag", "sent", "acked", "failed", "error_types",
            "ca2_est", "cs2_est", "total_msgs_cluster", "bytes_rate"
    };

    private static final String DDL_RUNS = """
            CREATE TABLE IF NOT EXISTS runs (
              run_id   TEXT PRIMARY KEY,
              manifest TEXT NOT NULL
            )""";

    private static final String DDL_SERIES = """
            CREATE TABLE IF NOT EXISTS series (
              run_id             TEXT    NOT NULL,
              ts_ms              INTEGER NOT NULL,
              lambda_leo         REAL,
              lambda_consumer    REAL,
              rho                REAL,
              wq_ms              REAL,
              pred_ack_ms        REAL,
              pred_e2e_ms        REAL,
              ack_mean_ms        REAL,
              ack_p50_ms         REAL,
              ack_p95_ms         REAL,
              ack_p99_ms         REAL,
              ack_max_ms         REAL,
              ack_count          INTEGER,
              ack_invocations    INTEGER,
              e2e_mean_ms        REAL,
              e2e_p50_ms         REAL,
              e2e_p99_ms         REAL,
              e2e_max_ms         REAL,
              e2e_count          INTEGER,
              err_ack_ms         REAL,
              err_e2e_ms         REAL,
              err_ack_rel        REAL,
              err_e2e_rel        REAL,
              consumer_lag       INTEGER,
              sent               INTEGER,
              acked              INTEGER,
              failed             INTEGER,
              error_types        TEXT,
              ca2_est            REAL,
              cs2_est            REAL,
              total_msgs_cluster INTEGER,
              bytes_rate         REAL,
              PRIMARY KEY (run_id, ts_ms)
            )""";

    private static final String DDL_ACK = """
            CREATE TABLE IF NOT EXISTS ack (
              run_id        TEXT    NOT NULL,
              window_end_ms INTEGER NOT NULL,
              invocations   INTEGER,
              sent          INTEGER,
              acked         INTEGER,
              failed        INTEGER,
              bytes         INTEGER,
              ack_mean_ms   REAL,
              ack_p50_ms    REAL,
              ack_p95_ms    REAL,
              ack_p99_ms    REAL,
              ack_max_ms    REAL,
              errors        TEXT,
              PRIMARY KEY (run_id, window_end_ms)
            )""";

    private static final String INSERT_SERIES = "INSERT OR REPLACE INTO series ("
            + "run_id, ts_ms, " + String.join(", ", SERIES_VALUE_COLUMNS)
            + ") VALUES (" + "?, ?, " + "?, ".repeat(SERIES_VALUE_COLUMNS.length - 1) + "?)";

    private static final String SELECT_SERIES = "SELECT ts_ms, run_id, " + String.join(", ", SERIES_VALUE_COLUMNS)
            + " FROM series WHERE run_id = ? ORDER BY ts_ms ASC";

    private static final String INSERT_ACK = "INSERT OR REPLACE INTO ack ("
            + "run_id, window_end_ms, invocations, sent, acked, failed, bytes, "
            + "ack_mean_ms, ack_p50_ms, ack_p95_ms, ack_p99_ms, ack_max_ms, errors"
            + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String SELECT_ACK = "SELECT window_end_ms, run_id, invocations, sent, acked, failed, bytes, "
            + "ack_mean_ms, ack_p50_ms, ack_p95_ms, ack_p99_ms, ack_max_ms, errors"
            + " FROM ack WHERE run_id = ? ORDER BY window_end_ms ASC";

    private static final String SELECT_RUN_IDS = "SELECT run_id FROM runs "
            + "ORDER BY json_extract(manifest, '$.startEpochMs') DESC, run_id DESC";

    public static final class StoreException extends RuntimeException {
        public StoreException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private final String dbPath;
    private final Connection conn;

    public RunStore(String dbPath) {
        this.dbPath = dbPath;
        File parent = new File(dbPath).getAbsoluteFile().getParentFile();
        if (parent != null && !parent.isDirectory() && !parent.mkdirs()) {
            LOG.warning("RunStore: could not create parent directory " + parent);
        }
        try {
            Class.forName("org.sqlite.JDBC");
            this.conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
            this.conn.setAutoCommit(true);
            try (Statement st = conn.createStatement()) {
                st.execute("PRAGMA journal_mode=WAL");
                st.execute("PRAGMA synchronous=NORMAL");
                st.execute(DDL_RUNS);
                st.execute(DDL_SERIES);
                st.execute(DDL_ACK);
            }
        } catch (ClassNotFoundException | SQLException e) {
            throw new StoreException("failed to open SQLite database at " + dbPath, e);
        }
        LOG.info("RunStore opened: " + dbPath + " (WAL)");
    }

    public String dbPath() {
        return dbPath;
    }


    public synchronized void saveRun(String manifestJson) {
        String runId = runIdOf(manifestJson);
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT OR REPLACE INTO runs (run_id, manifest) VALUES (?, ?)")) {
            ps.setString(1, runId);
            ps.setString(2, manifestJson);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new StoreException("saveRun failed for " + runId, e);
        }
    }

    public synchronized boolean updateRun(String manifestJson) {
        String runId = runIdOf(manifestJson);
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE runs SET manifest = ? WHERE run_id = ?")) {
            ps.setString(1, manifestJson);
            ps.setString(2, runId);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new StoreException("updateRun failed for " + runId, e);
        }
    }

    public synchronized List<String> listRunIds() {
        List<String> ids = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(SELECT_RUN_IDS);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                ids.add(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new StoreException("listRunIds failed", e);
        }
        return ids;
    }

    public synchronized String getManifest(String runId) {
        try (PreparedStatement ps = conn.prepareStatement("SELECT manifest FROM runs WHERE run_id = ?")) {
            ps.setString(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        } catch (SQLException e) {
            throw new StoreException("getManifest failed for " + runId, e);
        }
    }


    public synchronized void insertSeriesBatch(String runId, List<Tick> ticks) {
        if (ticks == null || ticks.isEmpty()) return;
        try {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(INSERT_SERIES)) {
                for (Tick t : ticks) {
                    if (t == null) continue;
                    int i = 1;
                    ps.setString(i++, runId);
                    ps.setLong(i++, t.tsMs());
                    setReal(ps, i++, t.lambdaLeo());
                    setReal(ps, i++, t.lambdaConsumer());
                    setReal(ps, i++, t.rho());
                    setReal(ps, i++, t.wqMs());
                    setReal(ps, i++, t.predAckMs());
                    setReal(ps, i++, t.predE2eMs());
                    setReal(ps, i++, t.ackMeanMs());
                    setReal(ps, i++, t.ackP50Ms());
                    setReal(ps, i++, t.ackP95Ms());
                    setReal(ps, i++, t.ackP99Ms());
                    setReal(ps, i++, t.ackMaxMs());
                    ps.setLong(i++, t.ackCount());
                    ps.setLong(i++, t.ackInvocations());
                    setReal(ps, i++, t.e2eMeanMs());
                    setReal(ps, i++, t.e2eP50Ms());
                    setReal(ps, i++, t.e2eP99Ms());
                    setReal(ps, i++, t.e2eMaxMs());
                    ps.setLong(i++, t.e2eCount());
                    setReal(ps, i++, t.errAckMs());
                    setReal(ps, i++, t.errE2eMs());
                    setReal(ps, i++, t.errAckRel());
                    setReal(ps, i++, t.errE2eRel());
                    ps.setLong(i++, t.consumerLag());
                    ps.setLong(i++, t.sent());
                    ps.setLong(i++, t.acked());
                    ps.setLong(i++, t.failed());
                    ps.setString(i++, t.errorTypes() == null ? "" : t.errorTypes());
                    setReal(ps, i++, t.ca2Est());
                    setReal(ps, i++, t.cs2Est());
                    ps.setLong(i++, t.totalMsgsCluster());
                    setReal(ps, i, t.bytesRate());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            conn.commit();
        } catch (SQLException e) {
            rollbackQuietly();
            throw new StoreException("insertSeriesBatch failed for " + runId, e);
        } finally {
            restoreAutoCommit();
        }
    }

    public synchronized List<Map<String, Object>> getSeries(String runId) {
        List<Map<String, Object>> rows = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(SELECT_SERIES)) {
            ps.setString(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("ts_ms", rs.getLong("ts_ms"));
                    row.put("run_id", rs.getString("run_id"));
                    row.put("lambda_leo", real(rs, "lambda_leo"));
                    row.put("lambda_consumer", real(rs, "lambda_consumer"));
                    row.put("rho", real(rs, "rho"));
                    row.put("wq_ms", real(rs, "wq_ms"));
                    row.put("pred_ack_ms", real(rs, "pred_ack_ms"));
                    row.put("pred_e2e_ms", real(rs, "pred_e2e_ms"));
                    row.put("ack_mean_ms", real(rs, "ack_mean_ms"));
                    row.put("ack_p50_ms", real(rs, "ack_p50_ms"));
                    row.put("ack_p95_ms", real(rs, "ack_p95_ms"));
                    row.put("ack_p99_ms", real(rs, "ack_p99_ms"));
                    row.put("ack_max_ms", real(rs, "ack_max_ms"));
                    row.put("ack_count", rs.getLong("ack_count"));
                    row.put("ack_invocations", rs.getLong("ack_invocations"));
                    row.put("e2e_mean_ms", real(rs, "e2e_mean_ms"));
                    row.put("e2e_p50_ms", real(rs, "e2e_p50_ms"));
                    row.put("e2e_p99_ms", real(rs, "e2e_p99_ms"));
                    row.put("e2e_max_ms", real(rs, "e2e_max_ms"));
                    row.put("e2e_count", rs.getLong("e2e_count"));
                    row.put("err_ack_ms", real(rs, "err_ack_ms"));
                    row.put("err_e2e_ms", real(rs, "err_e2e_ms"));
                    row.put("err_ack_rel", real(rs, "err_ack_rel"));
                    row.put("err_e2e_rel", real(rs, "err_e2e_rel"));
                    row.put("consumer_lag", rs.getLong("consumer_lag"));
                    row.put("sent", rs.getLong("sent"));
                    row.put("acked", rs.getLong("acked"));
                    row.put("failed", rs.getLong("failed"));
                    String errorTypes = rs.getString("error_types");
                    row.put("error_types", errorTypes == null ? "" : errorTypes);
                    row.put("ca2_est", real(rs, "ca2_est"));
                    row.put("cs2_est", real(rs, "cs2_est"));
                    row.put("total_msgs_cluster", rs.getLong("total_msgs_cluster"));
                    row.put("bytes_rate", real(rs, "bytes_rate"));
                    rows.add(row);
                }
            }
        } catch (SQLException e) {
            throw new StoreException("getSeries failed for " + runId, e);
        }
        return rows;
    }


    public synchronized void insertAck(String runId, AckSnapshot snapshot) {
        if (snapshot == null) return;
        try (PreparedStatement ps = conn.prepareStatement(INSERT_ACK)) {
            bindAck(ps, runId, snapshot);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new StoreException("insertAck failed for " + runId, e);
        }
    }

    public synchronized void insertAckBatch(String runId, List<AckSnapshot> snapshots) {
        if (snapshots == null || snapshots.isEmpty()) return;
        try {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(INSERT_ACK)) {
                for (AckSnapshot s : snapshots) {
                    if (s == null) continue;
                    bindAck(ps, runId, s);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            conn.commit();
        } catch (SQLException e) {
            rollbackQuietly();
            throw new StoreException("insertAckBatch failed for " + runId, e);
        } finally {
            restoreAutoCommit();
        }
    }

    public synchronized List<Map<String, Object>> getAck(String runId) {
        List<Map<String, Object>> rows = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(SELECT_ACK)) {
            ps.setString(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("window_end_ms", rs.getLong("window_end_ms"));
                    row.put("run_id", rs.getString("run_id"));
                    row.put("invocations", rs.getLong("invocations"));
                    row.put("sent", rs.getLong("sent"));
                    row.put("acked", rs.getLong("acked"));
                    row.put("failed", rs.getLong("failed"));
                    row.put("bytes", rs.getLong("bytes"));
                    row.put("ack_mean_ms", real(rs, "ack_mean_ms"));
                    row.put("ack_p50_ms", real(rs, "ack_p50_ms"));
                    row.put("ack_p95_ms", real(rs, "ack_p95_ms"));
                    row.put("ack_p99_ms", real(rs, "ack_p99_ms"));
                    row.put("ack_max_ms", real(rs, "ack_max_ms"));
                    row.put("errors", parseErrors(rs.getString("errors")));
                    rows.add(row);
                }
            }
        } catch (SQLException e) {
            throw new StoreException("getAck failed for " + runId, e);
        }
        return rows;
    }

    @Override
    public synchronized void close() {
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        } catch (SQLException e) {
            LOG.warning("RunStore close failed: " + e.getMessage());
        }
    }


    private static void bindAck(PreparedStatement ps, String runId, AckSnapshot a) throws SQLException {
        int i = 1;
        ps.setString(i++, runId);
        ps.setLong(i++, a.windowEnd());
        ps.setLong(i++, a.invocations());
        ps.setLong(i++, a.sent());
        ps.setLong(i++, a.acked());
        ps.setLong(i++, a.failed());
        ps.setLong(i++, a.bytes());
        setReal(ps, i++, a.mean());
        setReal(ps, i++, a.p50());
        setReal(ps, i++, a.p95());
        setReal(ps, i++, a.p99());
        setReal(ps, i++, a.max());
        ps.setString(i, errorsJson(a.errors()));
    }

    private static void setReal(PreparedStatement ps, int idx, double v) throws SQLException {
        if (Double.isNaN(v)) {
            ps.setNull(idx, Types.REAL);
        } else {
            ps.setDouble(idx, v);
        }
    }

    private static Double real(ResultSet rs, String column) throws SQLException {
        double v = rs.getDouble(column);
        return rs.wasNull() ? Double.NaN : v;
    }

    private static String errorsJson(Map<String, Long> errors) {
        try {
            return MAPPER.writeValueAsString(errors == null ? Map.of() : errors);
        } catch (Exception e) {
            return "{}";
        }
    }

    private static Map<String, Long> parseErrors(String json) {
        if (json == null || json.isBlank()) return new LinkedHashMap<>();
        try {
            Map<String, Long> parsed = MAPPER.readValue(json, ERRORS_TYPE);
            return parsed == null ? new LinkedHashMap<>() : parsed;
        } catch (Exception e) {
            return new LinkedHashMap<>();
        }
    }

    private static String runIdOf(String manifestJson) {
        try {
            JsonNode node = MAPPER.readTree(manifestJson);
            String runId = node == null ? null : node.path("runId").asText(null);
            if (runId == null || runId.isBlank()) {
                throw new IllegalArgumentException("manifest JSON has no runId");
            }
            return runId;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new StoreException("manifest is not valid JSON", e);
        }
    }

    private void rollbackQuietly() {
        try {
            if (!conn.getAutoCommit()) conn.rollback();
        } catch (SQLException ignored) {
        }
    }

    private void restoreAutoCommit() {
        try {
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            LOG.warning("RunStore: failed to restore auto-commit: " + e.getMessage());
        }
    }


    private static final String DDL_PROFILES = """
            CREATE TABLE IF NOT EXISTS profiles (
              name       TEXT PRIMARY KEY,
              config     TEXT NOT NULL,
              profile    TEXT NOT NULL,
              updated_ms INTEGER NOT NULL
            )""";

    private static final String DDL_ACTIVE_PROFILE = """
            CREATE TABLE IF NOT EXISTS active_profile (
              config TEXT PRIMARY KEY,
              name   TEXT NOT NULL
            )""";

    private void ensureProfileTables() {
        try (Statement st = conn.createStatement()) {
            st.execute(DDL_PROFILES);
            st.execute(DDL_ACTIVE_PROFILE);
        } catch (SQLException e) {
            throw new StoreException("profile tables init failed", e);
        }
    }

    public synchronized List<Map<String, Object>> listProfiles() {
        ensureProfileTables();
        List<Map<String, Object>> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT name, config, profile, updated_ms FROM profiles ORDER BY updated_ms DESC")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("name", rs.getString("name"));
                    row.put("config", rs.getString("config"));
                    row.put("updated_ms", rs.getLong("updated_ms"));
                    try {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> prof = MAPPER.readValue(rs.getString("profile"),
                                new TypeReference<Map<String, Object>>() { });
                        row.put("profile", prof);
                    } catch (Exception e) {
                        row.put("profile", Map.of());
                        row.put("unparseable", true);
                    }
                    out.add(row);
                }
            }
        } catch (SQLException e) {
            throw new StoreException("listProfiles failed", e);
        }
        return out;
    }

    public synchronized void saveProfile(String name, Object profile) {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("profile name required");
        ensureProfileTables();
        String json;
        String config;
        try {
            json = MAPPER.writeValueAsString(profile);
            config = MAPPER.readTree(json).path("config").asText("");
        } catch (Exception e) {
            throw new StoreException("profile is not serializable", e);
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT OR REPLACE INTO profiles (name, config, profile, updated_ms) VALUES (?, ?, ?, ?)")) {
            ps.setString(1, name);
            ps.setString(2, config);
            ps.setString(3, json);
            ps.setLong(4, System.currentTimeMillis());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new StoreException("saveProfile failed for " + name, e);
        }
    }

    public synchronized String getProfileJson(String name) {
        ensureProfileTables();
        try (PreparedStatement ps = conn.prepareStatement("SELECT profile FROM profiles WHERE name = ?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        } catch (SQLException e) {
            throw new StoreException("getProfile failed for " + name, e);
        }
    }

    public synchronized int deleteProfile(String name) {
        ensureProfileTables();
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM profiles WHERE name = ?")) {
            ps.setString(1, name);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new StoreException("deleteProfile failed for " + name, e);
        }
    }

    public synchronized String getActiveProfileName(String config) {
        ensureProfileTables();
        try (PreparedStatement ps = conn.prepareStatement("SELECT name FROM active_profile WHERE config = ?")) {
            ps.setString(1, config);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        } catch (SQLException e) {
            throw new StoreException("getActiveProfileName failed", e);
        }
    }

    public synchronized void setActiveProfileName(String config, String name) {
        ensureProfileTables();
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT OR REPLACE INTO active_profile (config, name) VALUES (?, ?)")) {
            ps.setString(1, config);
            ps.setString(2, name);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new StoreException("setActiveProfileName failed", e);
        }
    }


    private static final String DDL_WATCHED = """
            CREATE TABLE IF NOT EXISTS watched_topics (
              name TEXT PRIMARY KEY
            )""";

    private void ensureWatchedTable() {
        try (Statement st = conn.createStatement()) {
            st.execute(DDL_WATCHED);
        } catch (SQLException e) {
            throw new StoreException("watched_topics init failed", e);
        }
    }

    public synchronized List<String> getWatchedTopics() {
        ensureWatchedTable();
        List<String> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement("SELECT name FROM watched_topics ORDER BY name")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new StoreException("getWatchedTopics failed", e);
        }
        return out;
    }

    public synchronized void setWatchedTopics(List<String> names) {
        ensureWatchedTable();
        try (Statement st = conn.createStatement()) {
            st.execute("DELETE FROM watched_topics");
        } catch (SQLException e) {
            throw new StoreException("setWatchedTopics clear failed", e);
        }
        if (names == null) return;
        try (PreparedStatement ps = conn.prepareStatement("INSERT OR IGNORE INTO watched_topics (name) VALUES (?)")) {
            for (String n : names) {
                if (n == null || n.isBlank()) continue;
                ps.setString(1, n.strip());
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new StoreException("setWatchedTopics insert failed", e);
        }
    }
}
