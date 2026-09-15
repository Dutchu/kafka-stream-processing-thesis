package com.thesis.dashboard;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.thesis.dashboard.cluster.ClusterInspector;
import com.thesis.dashboard.cluster.ClusterSnapshot;
import com.thesis.dashboard.consumer.E2EConsumer;
import com.thesis.dashboard.consumer.E2ESnapshot;
import com.thesis.dashboard.control.AckAggregator;
import com.thesis.dashboard.control.AckSnapshot;
import com.thesis.dashboard.export.PrometheusExporter;
import com.thesis.dashboard.math.KingmanEngine;
import com.thesis.dashboard.math.KingmanResult;
import com.thesis.dashboard.math.Profile;
import com.thesis.dashboard.run.RunController;
import com.thesis.dashboard.run.RunManifest;
import io.javalin.Javalin;
import io.javalin.http.HttpStatus;
import io.javalin.websocket.WsContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.Node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DashboardApp {

    private static final Logger LOG = Logger.getLogger(DashboardApp.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    public static void main(String[] args) {
        Map<String, String> env = System.getenv();

        Map<String, String> fileCfg = readConfigFile(
                env.getOrDefault("CONFIG_FILE", "/opt/thesis-dashboard/cluster.env"));
        java.util.function.Function<String, String> opt = key -> {
            String v = env.getOrDefault(key, "");
            if (!v.isBlank()) return v;
            v = fileCfg.getOrDefault(key, "");
            return v.isBlank() ? null : v;
        };
        java.util.function.BiFunction<String, String, String> optOr = (key, def) -> {
            String v = opt.apply(key);
            return v != null ? v : def;
        };

        String kafkaSeed = optOr.apply("KAFKA_SEED", "10.0.0.6:9092");
        String kafkaBrokersEnv = env.getOrDefault("KAFKA_BROKERS", "");
        if (kafkaBrokersEnv.isBlank()) kafkaBrokersEnv = fileCfg.getOrDefault("KAFKA_BROKERS", "");
        String kafkaBrokers = kafkaBrokersEnv.isBlank()
                ? resolveBootstrap(kafkaSeed) : kafkaBrokersEnv;
        String functionUrl = optOr.apply("FUNCTION_URL", "");
        String config = optOr.apply("CONFIG", "1b-rf1");
        int dashboardPort = Integer.parseInt(env.getOrDefault("DASHBOARD_PORT", "8080"));
        String dbPath = env.getOrDefault("DB_PATH", "/var/lib/thesis-dashboard/thesis.db");
        String profilesFile = env.getOrDefault("PROFILES_FILE", "/opt/thesis-dashboard/profiles.json");
        String promUrl = env.getOrDefault("PROM_URL", "");
        String topicsRaw = opt.apply("TOPICS");
        if (topicsRaw == null) topicsRaw = env.getOrDefault("TOPICS", "weather-rain,weather-temp,weather-wind");
        List<String> topics = Arrays.asList(topicsRaw.split(","));
        String controlTopic = optOr.apply("CONTROL_TOPIC", "control-metrics");
        int e2eSampleEvery = Integer.parseInt(env.getOrDefault("E2E_SAMPLE_EVERY", "1"));
        long adminPollMs = Long.parseLong(env.getOrDefault("ADMIN_POLL_MS", "1000"));
        long topicDescribeMs = Long.parseLong(env.getOrDefault("TOPIC_DESCRIBE_MS", "5000"));

        LOG.info("Starting thesis-dashboard 2.0.0: config=" + config + " brokers=" + kafkaBrokers
                + " dbPath=" + dbPath + " profilesFile=" + profilesFile
                + " promUrl=" + (promUrl.isBlank() ? "(none)" : promUrl));

        ClusterInspector clusterInspector = new ClusterInspector(kafkaBrokers, topics, adminPollMs, topicDescribeMs);

        E2EConsumer e2eConsumer = new E2EConsumer(kafkaBrokers, topics, e2eSampleEvery);
        Thread e2eThread = new Thread(e2eConsumer, "e2e-consumer");
        e2eThread.setDaemon(true);
        e2eThread.start();

        AckAggregator ackAggregator = new AckAggregator(kafkaBrokers, controlTopic);
        Thread ackThread = new Thread(ackAggregator, "ack-aggregator");
        ackThread.setDaemon(true);
        ackThread.start();

        KingmanEngine kingmanEngine = new KingmanEngine(config, profilesFile);

        RunController runController = new RunController(dbPath, functionUrl, config, topics,
                ackAggregator, kingmanEngine, promUrl);

        try {
            List<String> watched = runController.store().getWatchedTopics();
            if (!watched.isEmpty()) {
                clusterInspector.setTopics(watched);
                e2eConsumer.retarget(watched);
                LOG.info("Watching persisted topics " + watched + " (env TOPICS ignored)");
            }
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Watched-topics restore failed, using env TOPICS: {0}", e.getMessage());
        }

        try {
            java.io.File pf = new java.io.File(profilesFile);
            if (pf.isFile()) {
                Map<String, Profile> fileMap = MAPPER.readValue(pf,
                        new com.fasterxml.jackson.core.type.TypeReference<Map<String, Profile>>() { });
                for (Map.Entry<String, Profile> e : fileMap.entrySet()) {
                    if (runController.store().getProfileJson(e.getKey()) == null && e.getValue() != null) {
                        runController.store().saveProfile(e.getKey(), e.getValue());
                        LOG.info("Profile library: imported '" + e.getKey() + "' from profiles file");
                    }
                }
            }
            if (runController.store().getProfileJson(config) == null) {
                Profile fileProfile = kingmanEngine.activeProfile();
                if (fileProfile != null) {
                    runController.store().saveProfile(config, fileProfile);
                }
            }
            String activeName = runController.store().getActiveProfileName(config);
            if (activeName == null) {
                runController.store().setActiveProfileName(config, config);
            } else {
                String json = runController.store().getProfileJson(activeName);
                if (json != null) {
                    try {
                        Profile p = new com.fasterxml.jackson.databind.ObjectMapper().readValue(json, Profile.class);
                        p.setConfig(config);
                        kingmanEngine.putProfile(p);
                    } catch (Exception e) {
                        LOG.log(Level.WARNING, "Active profile {0} unparseable, keeping file profile: {1}",
                                new Object[]{activeName, e.getMessage()});
                    }
                }
            }
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Profile library seed failed (continuing with file profile): {0}", e.getMessage());
        }

        com.thesis.dashboard.math.LiveProfiler liveProfiler = new com.thesis.dashboard.math.LiveProfiler(60);

        PrometheusExporter prometheusExporter = new PrometheusExporter();

        Map<WsContext, Boolean> wsClients = new ConcurrentHashMap<>();
        AtomicReference<Tick> lastTick = new AtomicReference<>(null);
        AtomicReference<AckSnapshot> lastPublishedAckBucket = new AtomicReference<>(null);
        java.util.concurrent.atomic.AtomicLong tickCount = new java.util.concurrent.atomic.AtomicLong(0);

        ScheduledExecutorService topologyExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cluster-topology-poller");
            t.setDaemon(true);
            return t;
        });
        clusterInspector.refreshTopology();
        topologyExecutor.scheduleAtFixedRate(clusterInspector::refreshTopology, topicDescribeMs, topicDescribeMs, TimeUnit.MILLISECONDS);

        ScheduledExecutorService offsetsExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cluster-offsets-poller");
            t.setDaemon(true);
            return t;
        });
        offsetsExecutor.scheduleAtFixedRate(clusterInspector::refreshOffsetsAndPublish, adminPollMs, adminPollMs, TimeUnit.MILLISECONDS);

        new java.io.File("/opt/thesis-frontend").mkdirs();
        Javalin app = Javalin.create(cfg -> {
            cfg.staticFiles.add(staticFiles -> {
                staticFiles.hostedPath = "/";
                staticFiles.directory = "/opt/thesis-frontend";
                staticFiles.location = io.javalin.http.staticfiles.Location.EXTERNAL;
            });
            cfg.staticFiles.add("/public");
        }).start("0.0.0.0", dashboardPort);
        LOG.info("Javalin dashboard listening on 0.0.0.0:" + dashboardPort);

        app.ws("/ws", ws -> {
            ws.onConnect(ctx -> {
                wsClients.put(ctx, Boolean.TRUE);
                Tick t = lastTick.get();
                if (t != null) {
                    try {
                        ctx.send(MAPPER.writeValueAsString(t));
                    } catch (Exception ignored) {
                    }
                }
            });
            ws.onClose(ctx -> wsClients.remove(ctx));
        });

        app.get("/api/health", ctx -> {
            ClusterSnapshot cs = clusterInspector.snapshot();
            ctx.json(Map.of(
                    "status", "ok",
                    "config", config,
                    "brokers", cs.brokers().size()));
        });

        app.get("/api/tick", ctx -> {
            Tick t = lastTick.get();
            if (t == null) {
                ctx.status(HttpStatus.SERVICE_UNAVAILABLE).json(Map.of("status", "no data yet"));
            } else {
                ctx.json(t);
            }
        });

        app.post("/api/run", ctx -> {
            try {
                RunController.RunRequest req = ctx.bodyAsClass(RunController.RunRequest.class);
                String runId = runController.startRun(req);
                ctx.json(Map.of("runId", runId));
            } catch (RunController.ValidationException e) {
                ctx.status(HttpStatus.BAD_REQUEST).json(Map.of("status", "error", "message", e.getMessage()));
            }
        });

        app.get("/api/run/active", ctx -> ctx.json(runController.currentRunStatus()));

        app.post("/api/run/{id}/abort", ctx -> {
            String id = ctx.pathParam("id");
            boolean ok = runController.abortRun(id);
            if (ok) {
                ctx.json(Map.of("status", "aborted", "runId", id,
                        "note", "in-flight function invocations run to durationSec; abort only marks the manifest"));
            } else {
                ctx.status(HttpStatus.NOT_FOUND).json(Map.of("status", "error", "message", "no such active run"));
            }
        });

        app.get("/api/topics", ctx -> ctx.json(clusterInspector.listAllTopics()));

        app.post("/api/topics/{name}/purge", ctx -> {
            ctx.json(clusterInspector.purgeTopic(ctx.pathParam("name")));
        });

        app.delete("/api/topics/{name}", ctx -> {
            Map<String, Object> res = clusterInspector.deleteTopic(ctx.pathParam("name"));
            if ("ok".equals(res.get("status"))) {
                ctx.json(res);
            } else {
                ctx.status(HttpStatus.BAD_REQUEST).json(res);
            }
        });

        app.get("/api/version", ctx -> {
            ctx.json(Map.of(
                    "backend", readFirstLine("/opt/thesis-deploy/current-sha"),
                    "frontend", readFirstLine("/opt/thesis-frontend/version.txt"),
                    "config", config));
        });

        app.get("/api/topics/watched", ctx -> ctx.json(Map.of(
                "watched", clusterInspector.watchedTopics(),
                "default", topics)));

        app.put("/api/topics/watched", ctx -> {
            try {
                Map<?, ?> body = ctx.bodyAsClass(Map.class);
                Object raw = body == null ? null : body.get("topics");
                List<String> names = new ArrayList<>();
                if (raw instanceof List<?> list) {
                    for (Object o : list) {
                        if (o != null && !String.valueOf(o).isBlank()) names.add(String.valueOf(o).strip());
                    }
                }
                runController.store().setWatchedTopics(names);
                clusterInspector.setTopics(names);
                e2eConsumer.retarget(names);
                ctx.json(Map.of("watched", clusterInspector.watchedTopics()));
            } catch (Exception e) {
                ctx.status(HttpStatus.BAD_REQUEST)
                        .json(Map.of("status", "error", "message", String.valueOf(e.getMessage())));
            }
        });

        app.get("/api/shape", ctx -> {
            ClusterSnapshot snap = clusterInspector.snapshot();
            Map<String, Integer> rf = new LinkedHashMap<>();
            for (ClusterSnapshot.PartitionInfo p : snap.partitions()) {
                rf.putIfAbsent(p.topic(), p.replicas().size());
            }
            int n = snap.brokers().size();
            int commonRf = rf.values().stream().findFirst().orElse(0);
            ctx.json(Map.of("brokers", n, "topicsRf", rf,
                    "suggestedConfig", n + "b-rf" + commonRf,
                    "profileConfig", config));
        });

        app.get("/api/runs", ctx -> ctx.json(runController.listRuns()));

        app.get("/api/runs/{id}/manifest", ctx -> {
            String json = runController.getManifest(ctx.pathParam("id"));
            if (json == null) {
                ctx.status(HttpStatus.NOT_FOUND).json(Map.of("status", "error", "message", "manifest not found"));
                return;
            }
            ctx.contentType("application/json").result(json);
        });

        app.get("/api/runs/{id}/summary", ctx -> {
            RunManifest manifest = runController.getManifestObject(ctx.pathParam("id"));
            if (manifest == null) {
                ctx.status(HttpStatus.NOT_FOUND).json(Map.of("status", "error", "message", "manifest not found"));
                return;
            }
            ctx.json(Map.of("status", manifest.status, "summary", manifest.summary == null ? Map.of() : manifest.summary));
        });

        app.get("/api/runs/{id}/series", ctx -> {
            String id = ctx.pathParam("id");
            if (runController.getManifest(id) == null) {
                ctx.status(HttpStatus.NOT_FOUND).json(Map.of("status", "error", "message", "run not found"));
                return;
            }
            ctx.json(runController.getSeries(id));
        });

        app.get("/api/runs/{id}/ack", ctx -> {
            String id = ctx.pathParam("id");
            if (runController.getManifest(id) == null) {
                ctx.status(HttpStatus.NOT_FOUND).json(Map.of("status", "error", "message", "run not found"));
                return;
            }
            ctx.json(runController.getAck(id));
        });

        app.get("/api/profile", ctx -> ctx.json(kingmanEngine.activeProfile()));
        app.put("/api/profile", ctx -> {
            Profile profile = ctx.bodyAsClass(Profile.class);
            kingmanEngine.putProfile(profile);
            Profile active = kingmanEngine.activeProfile();
            String name = (active.getSource() != null && !active.getSource().isBlank())
                    ? active.getSource() : config;
            runController.store().saveProfile(name, active);
            runController.store().setActiveProfileName(config, name);
            ctx.json(active);
        });

        app.post("/api/profile/from-run/{id}", ctx -> {
            try {
                Profile profile = runController.computeProfileFromRun(ctx.pathParam("id"));
                ctx.json(profile);
            } catch (RunController.ValidationException e) {
                ctx.status(HttpStatus.BAD_REQUEST).json(Map.of("status", "error", "message", e.getMessage()));
            }
        });

        app.get("/api/profile/candidates", ctx -> ctx.json(runController.calibrationCandidates()));

        app.get("/api/profiles", ctx -> ctx.json(runController.profileLibrary()));

        app.post("/api/profiles", ctx -> {
            String name = ctx.queryParam("name");
            if (name == null || name.isBlank()) {
                ctx.status(HttpStatus.BAD_REQUEST)
                        .json(Map.of("status", "error", "message", "query param ?name= is required"));
                return;
            }
            try {
                Profile p = MAPPER.readValue(ctx.body(), Profile.class);
                p.setConfig(config);
                runController.store().saveProfile(name.strip(), p);
                ctx.json(Map.of("status", "ok", "name", name.strip()));
            } catch (Exception e) {
                ctx.status(HttpStatus.BAD_REQUEST)
                        .json(Map.of("status", "error", "message", "invalid profile JSON: " + e.getMessage()));
            }
        });

        app.delete("/api/profiles/{name}", ctx -> {
            int n = runController.store().deleteProfile(ctx.pathParam("name"));
            if (n == 0) {
                ctx.status(HttpStatus.NOT_FOUND).json(Map.of("status", "error", "message", "unknown profile"));
            } else {
                ctx.json(Map.of("status", "ok"));
            }
        });

        app.post("/api/profile/active", ctx -> {
            try {
                Map<?, ?> body = ctx.bodyAsClass(Map.class);
                Object name = body == null ? null : body.get("name");
                Profile p = runController.activateProfile(name == null ? "" : String.valueOf(name));
                ctx.json(p);
            } catch (RunController.ValidationException e) {
                ctx.status(HttpStatus.BAD_REQUEST).json(Map.of("status", "error", "message", e.getMessage()));
            }
        });

        app.post("/api/profile/from-runs", ctx -> {
            try {
                RunController.FromRunsRequest req = ctx.bodyAsClass(RunController.FromRunsRequest.class);
                String dry = ctx.queryParam("dryRun");
                if ("true".equalsIgnoreCase(dry) || "1".equals(dry)) req.dryRun = true;
                ctx.json(runController.computeProfileFromRuns(req));
            } catch (RunController.ValidationException e) {
                ctx.status(HttpStatus.BAD_REQUEST).json(Map.of("status", "error", "message", e.getMessage()));
            }
        });

        app.get("/api/kingman/mode", ctx -> ctx.json(Map.of("mode", kingmanEngine.kingmanMode())));
        app.post("/api/kingman/mode", ctx -> {
            try {
                Map<?, ?> body = ctx.bodyAsClass(Map.class);
                Object mode = body == null ? null : body.get("mode");
                kingmanEngine.setKingmanMode(mode == null ? "" : String.valueOf(mode));
                ctx.json(Map.of("mode", kingmanEngine.kingmanMode()));
            } catch (IllegalArgumentException e) {
                ctx.status(HttpStatus.BAD_REQUEST).json(Map.of("status", "error", "message", e.getMessage()));
            }
        });

        app.get("/metrics", ctx -> ctx.contentType("text/plain; version=0.0.4").result(prometheusExporter.render()));

        ScheduledExecutorService mainLoop = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "main-loop-1hz");
            t.setDaemon(true);
            return t;
        });

        mainLoop.scheduleAtFixedRate(() -> {
            try {
                ClusterSnapshot cluster = clusterInspector.snapshot();
                E2ESnapshot e2e = e2eConsumer.snapshot();
                AckSnapshot ack = ackAggregator.snapshot();

                AckSnapshot previous = lastPublishedAckBucket.get();
                if (ack != null && ack.windowEnd() > 0 && (previous == null || ack.windowEnd() != previous.windowEnd())) {
                    runController.onAckBucketClosed(ack);
                    prometheusExporter.addSent(ack.sent());
                    prometheusExporter.addFailed(ack.failed());
                    lastPublishedAckBucket.set(ack);
                }

                KingmanResult kingman = kingmanEngine.evaluate(
                        cluster.lambdaLeo(), ack.mean(), e2e.mean(), e2e.ca2(), ackAggregator.runCs2Estimate());

                String errorTypes = formatErrors(ack.errors());

                Tick tick = new Tick(
                        System.currentTimeMillis(),
                        runController.currentRunStatus().runId(),
                        cluster.lambdaLeo(),
                        e2e.rate(),
                        kingman.rho(),
                        kingman.wqMs(),
                        kingman.predAckMs(),
                        kingman.predE2eMs(),
                        ack.mean(), ack.p50(), ack.p95(), ack.p99(), ack.max(),
                        ack.acked(), ack.invocations(),
                        e2e.mean(), e2e.p50(), e2e.p99(), e2e.max(), e2e.count(),
                        kingman.errAckMs(), kingman.errE2eMs(), kingman.errAckRel(), kingman.errE2eRel(),
                        e2e.lag(),
                        ack.sent(), ack.acked(), ack.failed(),
                        errorTypes,
                        e2e.ca2(), ackAggregator.runCs2Estimate(),
                        cluster.totalMsgs(), e2e.bytesRate(),
                        cluster,
                        runController.currentRunStatus(),
                        kingmanEngine.activeProfile()
                );

                lastTick.set(tick);

                runController.onTick(tick);

                try {
                    liveProfiler.observe(cluster.lambdaLeo(), ack.p50(), e2e.p50(),
                            e2e.ca2(), ackAggregator.runCs2Estimate());
                    tickCount.incrementAndGet();
                    if (tickCount.get() % 5 == 0) {
                        com.thesis.dashboard.math.LiveProfiler.LiveSnapshot snap = liveProfiler.snapshot();
                        if (snap != null) {
                            runController.store().saveProfile("live", new Profile(config,
                                    snap.muMsgs(), snap.tauAckMs(), snap.tauE2eMs(),
                                    snap.ca2(), snap.cs2(), "live-auto"));
                        }
                    }
                } catch (Exception e) {
                    LOG.log(Level.FINE, "live profiler persist failed: {0}", e.getMessage());
                }

                prometheusExporter.update(tick);
                prometheusExporter.setRunActive(tick.run().active(), tick.run().runId(), tick.run().exp());

                String json = MAPPER.writeValueAsString(tick);
                wsClients.keySet().forEach(ctx -> {
                    try {
                        if (ctx.session.isOpen()) {
                            ctx.send(json);
                        }
                    } catch (Exception e) {
                        LOG.log(Level.FINE, "WS send failed: {0}", e.getMessage());
                    }
                });
            } catch (Exception e) {
                LOG.log(Level.WARNING, "main loop tick failed: {0}", e.getMessage());
            }
        }, 0, 1, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            mainLoop.shutdown();
            offsetsExecutor.shutdown();
            topologyExecutor.shutdown();
            e2eConsumer.stop();
            ackAggregator.stop();
            clusterInspector.close();
            app.stop();
            runController.close();
        }));
    }

    private static String resolveBootstrap(String seed) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, seed);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 8_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 9_000);
        for (int attempt = 1; attempt <= 8; attempt++) {
            try (AdminClient admin = AdminClient.create(props)) {
                Collection<Node> nodes = admin.describeCluster().nodes()
                        .get(8, java.util.concurrent.TimeUnit.SECONDS);
                List<String> addrs = new ArrayList<>();
                for (Node n : nodes) {
                    if (n.host() != null) addrs.add(n.host() + ":" + n.port());
                }
                Collections.sort(addrs);
                if (!addrs.isEmpty()) {
                    String resolved = String.join(",", addrs);
                    LOG.info("Bootstrap resolved from seed " + seed + " (attempt " + attempt + "): " + resolved);
                    return resolved;
                }
            } catch (Exception e) {
                LOG.info("Bootstrap seed " + seed + " unreachable (attempt " + attempt + "/8): " + e.getMessage());
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        LOG.warning("Bootstrap resolution failed, using bare seed: " + seed);
        return seed;
    }

    private static Map<String, String> readConfigFile(String path) {
        Map<String, String> out = new LinkedHashMap<>();
        if (path == null || path.isBlank()) return out;
        java.io.File f = new java.io.File(path);
        if (!f.isFile() || !f.canRead()) {
            LOG.info("No config file at " + path + " (env-only mode)");
            return out;
        }
        try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(f))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.strip();
                if (line.isEmpty() || line.startsWith("#")) continue;
                int eq = line.indexOf('=');
                if (eq <= 0) continue;
                String k = line.substring(0, eq).strip();
                String v = line.substring(eq + 1).strip();
                if (v.length() >= 2 && ((v.startsWith("\"") && v.endsWith("\""))
                        || (v.startsWith("'") && v.endsWith("'")))) {
                    v = v.substring(1, v.length() - 1);
                }
                if (!k.isEmpty()) out.put(k, v);
            }
            LOG.info("Config file " + path + " loaded (" + out.size() + " keys)");
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Cannot read config file {0}: {1}",
                    new Object[]{path, e.getMessage()});
        }
        return out;
    }

    private static String readFirstLine(String path) {
        try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(path))) {
            String line = br.readLine();
            return (line == null || line.isBlank()) ? "unknown" : line.strip();
        } catch (Exception e) {
            return "unknown";
        }
    }

    private static String formatErrors(Map<String, Long> errors) {
        if (errors == null || errors.isEmpty()) return "";
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, Long> e : errors.entrySet()) {
            if (!first) sb.append(';');
            sb.append(e.getKey()).append(':').append(e.getValue());
            first = false;
        }
        return sb.toString();
    }
}
