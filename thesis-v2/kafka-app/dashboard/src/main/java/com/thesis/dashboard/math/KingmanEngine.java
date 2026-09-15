package com.thesis.dashboard.math;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KingmanEngine {

    private static final Logger LOG = Logger.getLogger(KingmanEngine.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private static final double RHO_CAP = 0.999;
    private static final int MAE_WINDOW_SECONDS = 60;

    private final String config;
    private final File profilesFile;
    private final Object lock = new Object();

    private Map<String, Profile> allProfiles = new HashMap<>();
    private volatile Profile activeProfile;
    private volatile String kingmanMode = "profile";

    private final Deque<Double> ackErrHistory = new ArrayDeque<>();
    private final Deque<Double> e2eErrHistory = new ArrayDeque<>();

    public KingmanEngine(String config, String profilesFilePath) {
        this.config = config;
        this.profilesFile = (profilesFilePath == null || profilesFilePath.isBlank()) ? null : new File(profilesFilePath);
        loadFromDisk();
    }

    private void loadFromDisk() {
        synchronized (lock) {
            if (profilesFile != null && profilesFile.isFile()) {
                try {
                    Map<String, Profile> loaded = MAPPER.readValue(profilesFile,
                            MAPPER.getTypeFactory().constructMapType(HashMap.class, String.class, Profile.class));
                    this.allProfiles = loaded;
                    Profile p = loaded.get(config);
                    this.activeProfile = (p != null) ? p : Profile.defaultProfile(config);
                    if (this.activeProfile.getConfig() == null) this.activeProfile.setConfig(config);
                    return;
                } catch (IOException e) {
                    LOG.log(Level.WARNING, "Failed to load PROFILES_FILE {0}: {1}",
                            new Object[]{profilesFile, e.getMessage()});
                }
            }
            this.activeProfile = Profile.defaultProfile(config);
        }
    }

    private void persistToDisk() {
        if (profilesFile == null) return;
        synchronized (lock) {
            try {
                File parent = profilesFile.getParentFile();
                if (parent != null) parent.mkdirs();
                MAPPER.writeValue(profilesFile, allProfiles);
            } catch (IOException e) {
                LOG.log(Level.WARNING, "Failed to persist PROFILES_FILE {0}: {1}",
                        new Object[]{profilesFile, e.getMessage()});
            }
        }
    }

    public void putProfile(Profile profile) {
        synchronized (lock) {
            profile.setConfig(config);
            this.activeProfile = profile;
            this.allProfiles.put(config, profile);
            persistToDisk();
        }
    }

    public Profile activeProfile() {
        return activeProfile;
    }

    public String kingmanMode() {
        return kingmanMode;
    }

    public void setKingmanMode(String mode) {
        if (!"profile".equals(mode) && !"live".equals(mode)) {
            throw new IllegalArgumentException("mode must be profile|live");
        }
        kingmanMode = mode;
    }

    public String config() {
        return config;
    }

    public synchronized KingmanResult evaluate(double lambda, double obsAckMeanMs, double obsE2eMeanMs,
                                                double ca2Est, double cs2Est) {
        Profile p = activeProfile;
        double mu = p.getMuMsgs();
        double tauAck = p.getTauAckMs();
        double tauE2e = p.getTauE2eMs();
        double ca2 = p.getCa2();
        double cs2 = p.getCs2();
        if ("live".equals(kingmanMode)) {
            if (Double.isFinite(ca2Est)) ca2 = ca2Est;
            if (Double.isFinite(cs2Est)) cs2 = cs2Est;
        }

        double rho = (mu > 0) ? lambda / mu : Double.NaN;
        boolean overload = Double.isFinite(rho) && rho >= 1.0;
        double rhoEff = Double.isFinite(rho) ? Math.min(rho, RHO_CAP) : Double.NaN;

        double wq;
        if (!Double.isFinite(rhoEff) || rhoEff <= 0 || !Double.isFinite(ca2) || !Double.isFinite(cs2)
                || !Double.isFinite(tauAck)) {
            wq = Double.NaN;
        } else {
            wq = (rhoEff / (1.0 - rhoEff)) * ((ca2 + cs2) / 2.0) * tauAck;
        }

        double predAck = Double.isFinite(tauAck) && Double.isFinite(wq) ? tauAck + wq : Double.NaN;
        double predE2e = Double.isFinite(tauE2e) && Double.isFinite(wq) ? tauE2e + wq : Double.NaN;

        double errAck = (Double.isFinite(obsAckMeanMs) && Double.isFinite(predAck)) ? obsAckMeanMs - predAck : Double.NaN;
        double errE2e = (Double.isFinite(obsE2eMeanMs) && Double.isFinite(predE2e)) ? obsE2eMeanMs - predE2e : Double.NaN;
        double errAckRel = (Double.isFinite(errAck) && predAck != 0) ? errAck / predAck : Double.NaN;
        double errE2eRel = (Double.isFinite(errE2e) && predE2e != 0) ? errE2e / predE2e : Double.NaN;

        pushBounded(ackErrHistory, errAck);
        pushBounded(e2eErrHistory, errE2e);
        double maeAck = mean(ackErrHistory, true);
        double maeE2e = mean(e2eErrHistory, true);

        return new KingmanResult(lambda, mu, rho, rhoEff, overload, wq, predAck, predE2e,
                obsAckMeanMs, obsE2eMeanMs, errAck, errE2e, errAckRel, errE2eRel, maeAck, maeE2e,
                ca2, cs2, ca2Est, cs2Est);
    }

    private static void pushBounded(Deque<Double> deque, double value) {
        deque.addLast(value);
        while (deque.size() > MAE_WINDOW_SECONDS) {
            deque.removeFirst();
        }
    }

    private static double mean(Deque<Double> deque, boolean absolute) {
        double sum = 0;
        int n = 0;
        for (double v : deque) {
            if (!Double.isFinite(v)) continue;
            sum += absolute ? Math.abs(v) : v;
            n++;
        }
        return n > 0 ? sum / n : Double.NaN;
    }
}
