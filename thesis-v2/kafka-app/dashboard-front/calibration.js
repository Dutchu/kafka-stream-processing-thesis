(function () {
    'use strict';

    const PARAMS = ['tauAckMs', 'tauE2eMs', 'cs2', 'ca2', 'muMsgs'];
    const UNITS = { tauAckMs: ' ms', tauE2eMs: ' ms', cs2: '', ca2: '', muMsgs: ' msg/s' };

    function qs(id) { return document.getElementById(id); }

    function esc(s) {
        return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({
            '&': '&amp;', '<': '&lt;', '&gt;': '&gt;', '"': '&quot;', "'": '&#39;'
        }[c]));
    }

    async function fetchJson(url, options) {
        const resp = await fetch(url, options);
        let body = null;
        try { body = await resp.json(); } catch (_) {                    }
        if (!resp.ok) {
            const message = (body && body.message) ? body.message : ('HTTP ' + resp.status);
            throw new Error(message);
        }
        return body;
    }

    function fmt(v, digits) {
        if (v === null || v === undefined) return 'NaN';
        const n = Number(v);
        return Number.isFinite(n) ? n.toFixed(digits == null ? 3 : digits) : 'NaN';
    }

    function selectedOf(param) {
        const card = document.querySelector(`.calib-card[data-param="${param}"]`);
        if (!card) return [];
        return Array.from(card.querySelectorAll('input[type="checkbox"]:checked')).map(el => el.value);
    }

    function allSelected() {
        return {
            tauAckRuns: selectedOf('tauAckMs'),
            tauE2eRuns: selectedOf('tauE2eMs'),
            cs2Runs: selectedOf('cs2'),
            ca2Runs: selectedOf('ca2'),
            muRuns: selectedOf('muMsgs')
        };
    }

    function selectionRule() {
        const sel = allSelected();
        const counts = {
            tauAckMs: sel.tauAckRuns.length, tauE2eMs: sel.tauE2eRuns.length,
            cs2: sel.cs2Runs.length, ca2: sel.ca2Runs.length, muMsgs: sel.muRuns.length
        };
        const bad = Object.entries(counts).filter(([, n]) => n > 0 && n < 3).map(([k]) => k);
        const total = Object.values(counts).reduce((a, b) => a + b, 0);
        return { bad, total };
    }

    function renderStats(stats) {
        PARAMS.forEach(p => {
            const card = document.querySelector(`.calib-card[data-param="${p}"]`);
            if (!card) return;
            const line = card.querySelector('.calib-result');
            const st = stats ? stats[p] : null;
            if (!st) {
                line.innerHTML = 'wybierz biegi…';
                return;
            }
            line.innerHTML = `mediana <strong>${esc(fmt(st.value))}${esc(UNITS[p])}</strong>` +
                ` ± ${esc(fmt(st.sd))} (bezwzgl.), CoV ${esc(fmt(st.cov * 100, 1))}% (wzgl.), n=${esc(st.n)}`;
        });
    }

    async function refreshPreview() {
        const status = qs('calib-status');
        const { bad, total } = selectionRule();
        const saveBtn = qs('btn-calib-save');
        const applyBtn = qs('btn-calib-apply');
        if (total === 0) {
            if (status) status.innerText = 'Zaznacz biegi (min. 3 na parametr).';
            [saveBtn, applyBtn].forEach(b => { if (b) b.disabled = true; });
            renderStats(null);
            return;
        }
        if (bad.length > 0) {
            if (status) status.innerText = `Za mało biegów: ${bad.join(', ')} (min. 3 na parametr).`;
            [saveBtn, applyBtn].forEach(b => { if (b) b.disabled = true; });
            return;
        }
        [saveBtn, applyBtn].forEach(b => { if (b) b.disabled = false; });
        try {
            const body = allSelected();
            const res = await fetchJson(dashUrl('/api/profile/from-runs?dryRun=true'), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body)
            });
            renderStats(res && res.stats);
            if (status) status.innerText = 'Podgląd (nie zapisano).';
        } catch (err) {
            if (status) status.innerText = 'Błąd podglądu: ' + err.message;
        }
    }

    async function loadCandidates() {
        try {
            const cands = await fetchJson(dashUrl('/api/profile/candidates'));
            PARAMS.forEach(p => {
                const card = document.querySelector(`.calib-card[data-param="${p}"]`);
                if (!card) return;
                const list = card.querySelector('.calib-list');
                const rows = (cands && cands[p]) || [];
                if (rows.length === 0) {
                    list.innerHTML = '<li class="hint">brak kwalifikujących się biegów</li>';
                    return;
                }
                list.innerHTML = rows.map(r => {
                    const extra = r.parallelism != null ? ` P=${esc(r.parallelism)}` : '';
                    const lbl = `${esc(r.runId)} <span class="hint">${esc(r.exp)}${extra} · ${esc(fmt(r.value))}${esc(UNITS[p])}</span>`;
                    return `<li><label><input type="checkbox" value="${esc(r.runId)}"> ${lbl}</label></li>`;
                }).join('');
                list.querySelectorAll('input[type="checkbox"]').forEach(cb => {
                    cb.addEventListener('change', refreshPreview);
                });
                const note = card.querySelector('.calib-skipped');
                if (note) note.remove();
            });
        } catch (err) {
            const status = qs('calib-status');
            if (status) status.innerText = 'Błąd pobierania kandydatów: ' + err.message;
        }
        refreshPreview();
    }

    async function onSave() {
        const status = qs('calib-status');
        try {
            const nameInput = qs('calib-name');
            const body = allSelected();
            const custom = nameInput ? nameInput.value.trim() : '';
            if (custom) body.name = custom;
            const res = await fetchJson(dashUrl('/api/profile/from-runs'), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body)
            });
            renderStats(res && res.stats);
            const prof = res && res.profile;
            const st = (res && res.stats) || {};
            const parts = Object.entries(st).map(entry => {
                const k = entry[0], v = entry[1];
                return k + '=' + fmt(v.value) + ' ± ' + fmt(v.sd) + ' (n=' + v.n + ')';
            });
            if (status) {
                if (prof) {
                    status.innerHTML = '<strong>Zapisano profil "'
                        + esc((res && res.name) || (prof.source || '')) + '" (tylko w bibliotece).</strong><br>'
                        + esc(parts.join(' · ') || 'brak parametrów')
                        + '<br><span class="hint">Aktywuj go na karcie Wykresy, żeby liczył predykcje.</span>';
                } else {
                    status.innerText = 'Zapisano (brak profilu w odpowiedzi).';
                }
            }
            if (typeof window.refreshProfileSelector === 'function') window.refreshProfileSelector();
        } catch (err) {
            if (status) status.innerText = 'Błąd zapisu: ' + err.message;
        }
    }

    async function loadMode() {
        try {
            const m = await fetchJson(dashUrl('/api/kingman/mode'));
            const radio = document.querySelector(`input[name="kingman-mode"][value="${m && m.mode}"]`);
            if (radio) radio.checked = true;
        } catch (_) {                     }
    }

    async function onModeChange(evt) {
        try {
            await fetchJson(dashUrl('/api/kingman/mode'), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ mode: evt.target.value })
            });
        } catch (err) {
            const status = qs('calib-status');
            if (status) status.innerText = 'Błąd przełącznika: ' + err.message;
            loadMode();
        }
    }

    async function checkOmb() {
        const btn = qs('btn-omb-import');
        const note = qs('omb-note');
        if (!btn) return;
        btn.disabled = true;
        try {
            const resp = await fetch(dashUrl('/profiles.json'));
            if (!resp.ok) throw new Error('HTTP ' + resp.status);
            const all = await resp.json();
            const omb = all && all.OMB;
            if (!omb || typeof omb !== 'object') throw new Error('no OMB key');
            const mu = (omb.muMsgs === undefined || omb.muMsgs === null) ? '—' : fmt(omb.muMsgs);
            const tau = (omb.tauAckMs === undefined || omb.tauAckMs === null) ? '—' : fmt(omb.tauAckMs);
            btn.disabled = false;
            btn.title = 'Importuj profil OMB do biblioteki i aktywuj';
            if (note) note.innerText = `OMB znaleziony: μ=${mu} msg/s, τ_ack=${tau} ms.`;
        } catch (_) {
            btn.disabled = true;
            btn.title = 'Brak /profiles.json z kluczem OMB — uruchom omb_import.py i redeploy';
            if (note) note.innerText = 'brak profilu OMB (uruchom omb_import.py i redeploy aplikacji)';
        }
    }

    async function onOmbImport() {
        const status = qs('calib-status');
        try {
            const resp = await fetch(dashUrl('/profiles.json'));
            const all = await resp.json();
            const omb = all && all.OMB;
            if (!omb) throw new Error('no OMB key in /profiles.json');
            await fetchJson(dashUrl('/api/profiles?name=OMB'), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(omb)
            });
            await fetchJson(dashUrl('/api/profile/active'), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name: 'OMB' })
            });
            await fetchJson(dashUrl('/api/kingman/mode'), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ mode: 'profile' })
            });
            loadMode();
            if (typeof window.refreshProfileSelector === 'function') window.refreshProfileSelector();
            if (status) status.innerText = 'Profil OMB zaimportowany i aktywny (tryb: profil).';
        } catch (err) {
            if (status) status.innerText = 'Błąd importu OMB: ' + err.message;
        }
    }

    function showTab(which) {
        const tabs = { charts: 'tab-charts', control: 'tab-control', calib: 'tab-calib' };
        Object.entries(tabs).forEach(([name, id]) => {
            const el = qs(id);
            if (el) el.hidden = (name !== which);
        });
        const btns = { charts: 'tab-btn-charts', control: 'tab-btn-control', calib: 'tab-btn-calib' };
        Object.entries(btns).forEach(([name, id]) => {
            const el = qs(id);
            if (el) el.classList.toggle('active', name === which);
        });
        if (which === 'calib') { loadCandidates(); loadMode(); checkOmb(); }
    }
    window.showTab = showTab;

    document.addEventListener('DOMContentLoaded', () => {
        const chartsBtn = qs('tab-btn-charts');
        const controlBtn = qs('tab-btn-control');
        const calibBtn = qs('tab-btn-calib');
        if (chartsBtn) chartsBtn.addEventListener('click', () => showTab('charts'));
        if (controlBtn) controlBtn.addEventListener('click', () => showTab('control'));
        if (calibBtn) calibBtn.addEventListener('click', () => showTab('calib'));
        const saveBtn = qs('btn-calib-save');
        if (saveBtn) saveBtn.addEventListener('click', refreshPreview);
        const applyBtn = qs('btn-calib-apply');
        if (applyBtn) applyBtn.addEventListener('click', onSave);
        document.querySelectorAll('input[name="kingman-mode"]').forEach(r => {
            r.addEventListener('change', onModeChange);
        });
        const ombBtn = qs('btn-omb-import');
        if (ombBtn) ombBtn.addEventListener('click', onOmbImport);
    });
})();
