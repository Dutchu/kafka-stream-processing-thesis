package com.thesis.dashboard.math;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KingmanEngineTest {

    private static final double EPS = 1e-9;

    private KingmanEngine engineWithProfile(double muMsgs, double tauAckMs, double tauE2eMs, double ca2, double cs2) {
        KingmanEngine engine = new KingmanEngine("test-config", null);
        engine.putProfile(new Profile("test-config", muMsgs, tauAckMs, tauE2eMs, ca2, cs2, "unit-test"));
        return engine;
    }

    @Test
    void wqMatchesReferenceValue_rho50_ca1_cs05_tau6() {
        KingmanEngine engine = engineWithProfile(100.0, 6.0, 20.0, 1.0, 0.5);
        KingmanResult result = engine.evaluate(50.0, Double.NaN, Double.NaN, Double.NaN, Double.NaN);

        assertEquals(0.5, result.rho(), EPS);
        assertEquals(4.5, result.wqMs(), 1e-6, "Wq(rho=0.5, ca2=1, cs2=0.5, tau=6) must equal 4.5 ms");
        assertEquals(6.0 + 4.5, result.predAckMs(), 1e-6);
        assertEquals(20.0 + 4.5, result.predE2eMs(), 1e-6);
        assertFalse(result.overload());
    }

    @Test
    void rhoAtOrAboveOneCapsAtPointNineNineNineAndFlagsOverload() {
        KingmanEngine engine = engineWithProfile(100.0, 6.0, 20.0, 1.0, 0.5);

        KingmanResult atCapacity = engine.evaluate(100.0, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
        assertEquals(1.0, atCapacity.rho(), EPS);
        assertEquals(0.999, atCapacity.rhoEff(), EPS);
        assertTrue(atCapacity.overload());

        KingmanEngine engine2 = engineWithProfile(100.0, 6.0, 20.0, 1.0, 0.5);
        KingmanResult overCapacity = engine2.evaluate(150.0, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
        assertEquals(1.5, overCapacity.rho(), EPS);
        assertEquals(0.999, overCapacity.rhoEff(), EPS);
        assertTrue(overCapacity.overload());
        assertTrue(Double.isFinite(overCapacity.wqMs()), "Wq must stay finite (rhoEff capped) even when rho > 1");
    }

    @Test
    void missingProfileIsNanSafe() {
        KingmanEngine engine = new KingmanEngine("no-profile-config", null);
        KingmanResult result = engine.evaluate(50.0, 12.0, 30.0, 0.9, 0.6);

        assertTrue(Double.isNaN(result.mu()));
        assertTrue(Double.isNaN(result.rho()));
        assertTrue(Double.isNaN(result.wqMs()));
        assertTrue(Double.isNaN(result.predAckMs()));
        assertTrue(Double.isNaN(result.predE2eMs()));
        assertFalse(result.overload());
    }

    @Test
    void errorsAreNanWhenObservedValuesAreNan() {
        KingmanEngine engine = engineWithProfile(100.0, 6.0, 20.0, 1.0, 0.5);
        KingmanResult result = engine.evaluate(50.0, Double.NaN, Double.NaN, Double.NaN, Double.NaN);

        assertTrue(Double.isNaN(result.errAckMs()));
        assertTrue(Double.isNaN(result.errE2eMs()));
        assertTrue(Double.isNaN(result.errAckRel()));
        assertTrue(Double.isNaN(result.errE2eRel()));
    }

    @Test
    void errorsComputedWhenObservedValuesPresent() {
        KingmanEngine engine = engineWithProfile(100.0, 6.0, 20.0, 1.0, 0.5);
        KingmanResult result = engine.evaluate(50.0, 12.0, 30.0, 0.9, 0.6);

        assertEquals(1.5, result.errAckMs(), 1e-6);
        assertEquals(1.5 / 10.5, result.errAckRel(), 1e-6);
        assertEquals(0.9, result.ca2Est(), 1e-9, "live ca2Est must be passed through unchanged");
        assertEquals(0.6, result.cs2Est(), 1e-9, "live cs2Est must be passed through unchanged");
    }

    @Test
    void liveModeUsesTickEstimatesAndProfileModeUsesFrozenValues() {
        KingmanEngine engine = engineWithProfile(100.0, 6.0, 20.0, 1.0, 0.5);
        assertEquals("profile", engine.kingmanMode());

        KingmanResult fromProfile = engine.evaluate(50.0, Double.NaN, Double.NaN, 0.2, 0.2);
        assertEquals(4.5, fromProfile.wqMs(), 1e-6, "profile mode ignores live estimates");

        engine.setKingmanMode("live");
        assertEquals("live", engine.kingmanMode());
        KingmanResult fromLive = engine.evaluate(50.0, Double.NaN, Double.NaN, 0.2, 0.2);
        assertEquals(1.2, fromLive.wqMs(), 1e-6, "live mode feeds tick estimates into Wq");

        KingmanResult fallback = engine.evaluate(50.0, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
        assertEquals(4.5, fallback.wqMs(), 1e-6);
    }

    @Test
    void profileNeverChangesWithoutExplicitPutOrFromRun() {
        KingmanEngine engine = engineWithProfile(100.0, 6.0, 20.0, 1.0, 0.5);
        Profile before = engine.activeProfile();

        engine.evaluate(80.0, 11.0, 28.0, 0.5, 0.9);
        engine.evaluate(20.0, 5.0, 15.0, 2.0, 0.1);

        Profile after = engine.activeProfile();
        assertEquals(before.getMuMsgs(), after.getMuMsgs());
        assertEquals(before.getCa2(), after.getCa2());
        assertEquals(before.getCs2(), after.getCs2());
        assertEquals("unit-test", after.getSource());
    }
}
