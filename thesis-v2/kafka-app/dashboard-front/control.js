(function () {
    'use strict';

    function qs(id) { return document.getElementById(id); }

    function setError(msg) {
        const el = qs('form-error');
        if (el) el.innerText = msg || '';
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

    function selectedTopics(form) {
        return Array.from(form.querySelectorAll('input[name="topic"]:checked')).map(el => el.value);
    }

    async function onSubmit(evt) {
        evt.preventDefault();
        setError('');
        const form = qs('run-form');
        if (!form) return;

        const mode = form.querySelector('input[name="mode"]:checked');
        const modeValue = mode ? mode.value : 'duration';
        const value = Number(qs('field-value').value);

        const body = {
            exp: qs('field-exp').value,
            label: qs('field-label').value,
            parallelism: Number(qs('field-parallelism').value),
            ratePerSec: Number(qs('field-rate').value),
            topics: selectedTopics(form),
            intensity: Number(qs('field-intensity').value),
            runIndex: Number(qs('field-run-index').value)
        };
        if (modeValue === 'duration') {
            body.durationSec = value;
        } else {
            body.count = value;
        }

        try {
            const result = await fetchJson(dashUrl('/api/run'), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body)
            });
            console.log('run started:', result && result.runId);
            setTimeout(refreshRunList, 500);
        } catch (err) {
            setError('Błąd startu biegu: ' + err.message);
        }
    }

    async function onAbort() {
        setError('');
        try {
            const active = await fetchJson(dashUrl('/api/run/active'));
            if (!active || !active.active) {
                setError('Brak aktywnego biegu.');
                return;
            }
            await fetchJson(dashUrl(`/api/run/${encodeURIComponent(active.runId)}/abort`), { method: 'POST' });
            setTimeout(refreshRunList, 500);
        } catch (err) {
            setError('Błąd przerwania biegu: ' + err.message);
        }
    }

    async function refreshRunList() {
        const list = qs('run-list');
        if (!list) return;
        try {
            const runs = await fetchJson(dashUrl('/api/runs'));
            if (!Array.isArray(runs) || runs.length === 0) {
                list.innerHTML = '<li class="hint">brak jeszcze biegów</li>';
                return;
            }
            const api = dashUrl('/api/runs/');
            list.innerHTML = runs.slice(0, 10).map(runId => {
                const id = encodeURIComponent(runId);
                return `<li><strong>${escapeHtml(runId)}</strong>` +
                       `<span><a href="${api}${id}/manifest" target="_blank">manifest</a>` +
                       `<a href="${api}${id}/series" target="_blank">series</a>` +
                       `<a href="${api}${id}/ack" target="_blank">ack</a>` +
                       `<a href="${api}${id}/summary" target="_blank">summary</a></span></li>`;
            }).join('');
        } catch (err) {
            console.warn('control: failed to refresh run list', err);
            list.innerHTML = '<li class="hint">błąd pobierania listy biegów</li>';
        }
    }

    function escapeHtml(s) {
        return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({
            '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
        }[c]));
    }

    let wasActive = false;
    window.onTickForControl = function (tick) {
        const active = !!(tick && tick.run && tick.run.active);
        if (wasActive && !active) {
            refreshRunList();
            showExportBanner();
        }
        wasActive = active;
    };

    function runDirFor(manifest) {
        const exp = manifest.exp || 'FREE';
        const config = manifest.config || 'unknown';
        const runIndex = manifest.runIndex || 1;
        let dir = `thesis-v2/results/${exp}/${config}/run${runIndex}`;
        let note = '';
        if (exp === 'E4' && manifest.parallelism) {
            dir = `thesis-v2/results/${exp}/${config}/P${manifest.parallelism}/run${runIndex}`;
        } else if (exp === 'E2') {
            dir = `thesis-v2/results/${exp}/${config}/rho…/run${runIndex}`;
            note = ' (podmień rho… na szczebel: rho25/50/75/90)';
        }
        return { dir, note };
    }

    async function showExportBanner() {
        const banner = qs('export-banner');
        if (!banner) return;
        try {
            const runs = await fetchJson(dashUrl('/api/runs'));
            if (!Array.isArray(runs) || runs.length === 0) return;
            const runId = runs[0];
            const [manifest, summary] = await Promise.all([
                fetchJson(dashUrl(`/api/runs/${encodeURIComponent(runId)}/manifest`)),
                fetchJson(dashUrl(`/api/runs/${encodeURIComponent(runId)}/summary`))
            ]);
            const s = (summary && summary.summary) || {};
            const status = (summary && summary.status) || (manifest && manifest.status) || '?';
            const { dir, note } = runDirFor(manifest || {});
            const cmd = `COLLECT_VIA_SSH=1 bash thesis-v2/gcloud/collect_run_artifacts.sh ${dir} ${runId}`;
            const line = `sent=${s.sent ?? '?'} acked=${s.acked ?? '?'} failed=${s.failed ?? '?'} status=${status}`;
            banner.innerHTML =
                `<div class="banner-head"><strong>Bieg gotowy do pobrania</strong>` +
                `<button type="button" id="btn-clear-export" title="Opróżnij szufladkę">✕</button></div>` +
                `<div>${escapeHtml(runId)}</div>` +
                `<span class="hint">${escapeHtml(line)}${escapeHtml(note)}</span>` +
                `<pre class="export-cmd"><code id="export-cmd-text">${escapeHtml(cmd)}</code></pre>` +
                `<button type="button" id="btn-copy-export" class="secondary">Kopiuj komendę</button> `;
            banner.hidden = false;
            const clearBtn = qs('btn-clear-export');
            if (clearBtn) clearBtn.addEventListener('click', () => {
                banner.hidden = true;
                banner.innerHTML = '';
            });
            const copyBtn = qs('btn-copy-export');
            if (copyBtn) copyBtn.addEventListener('click', async () => {
                const text = qs('export-cmd-text');
                const value = text ? text.innerText : cmd;
                try {
                    await navigator.clipboard.writeText(value);
                    copyBtn.innerText = 'Skopiowano ✓';
                } catch (_) {
                    copyBtn.innerText = 'Zaznacz ręcznie (Ctrl+C)';
                }
                setTimeout(() => { copyBtn.innerText = 'Kopiuj komendę'; }, 2500);
            });
        } catch (err) {
            console.warn('control: failed to show export banner', err);
        }
    }

    document.addEventListener('DOMContentLoaded', () => {
        const form = qs('run-form');
        if (form) form.addEventListener('submit', onSubmit);
        const abortBtn = qs('btn-abort');
        if (abortBtn) abortBtn.addEventListener('click', onAbort);
        const expSel = qs('field-exp');
        if (expSel) expSel.addEventListener('change', applyExpPreset);
        document.querySelectorAll('input[name="mode"]').forEach(r => {
            r.addEventListener('change', updateValueLabel);
        });
        applyExpPreset();
        refreshRunList();
    });

    const E2_RHOS = [0.25, 0.5, 0.75, 0.9];

    function setMode(modeValue) {
        const radio = document.querySelector(`input[name="mode"][value="${modeValue}"]`);
        if (radio) radio.checked = true;
        updateValueLabel();
    }

    function updateValueLabel() {
        const mode = document.querySelector('input[name="mode"]:checked');
        const lbl = qs('field-value-label');
        const val = qs('field-value');
        const isCount = !!(mode && mode.value === 'count');
        if (lbl) lbl.innerText = isCount
            ? 'Liczba wiadomości (≥1, w tempie z pola Tempo)'
            : 'Czas trwania biegu (s, 10–540)';
        if (val) {
            if (isCount) {
                val.min = '1';
                val.removeAttribute('max');
            } else {
                val.min = '10';
                val.max = '540';
            }
        }
    }

    function setHint(html) {
        const el = qs('exp-hint');
        if (el) el.innerHTML = html || '';
    }

    async function applyExpPreset() {
        const exp = qs('field-exp') ? qs('field-exp').value : 'FREE';
        setMode('duration');
        if (exp === 'E1') {
            qs('field-label').value = 'e1';
            qs('field-parallelism').value = 1;
            qs('field-rate').value = 20;
            qs('field-value').value = 90;
            setHint('E1 — kalibracja: 3 biegi (runIndex 1–3), potem „Zapisz profil z ostatniego biegu".');
        } else if (exp === 'E4') {
            qs('field-label').value = 'p10';
            qs('field-parallelism').value = 10;
            qs('field-rate').value = 0;
            qs('field-value').value = 120;
            setHint('E4 — drabinka P = 10 → 25 → 50 → 100 (po jednym biegu na szczebel, label p10/p25/p50/p100); między szczeblami wyczyść tematy; stop gdy λ stoi przez 2 P i zapali się sygnatura.');
        } else if (exp === 'E2') {
            qs('field-label').value = 'rho25';
            qs('field-parallelism').value = 20;
            qs('field-value').value = 120;
            let muTxt = 'μ nieznane (najpierw E4 + profil)';
            try {
                const prof = await fetchJson(dashUrl('/api/profile'));
                const mu = prof && Number(prof.muMsgs);
                if (Number.isFinite(mu) && mu > 0) {
                    const rates = E2_RHOS.map(r => `ρ=${r} → rate=${Math.round(r * mu / 20)}`).join(', ');
                    muTxt = `μ=${Math.round(mu)} msg/s ⇒ ${rates} (P=20)`;
                    qs('field-rate').value = Math.round(0.25 * mu / 20);
                }
            } catch (_) {                                                    }
            setHint(`E2 — drabinka ρ (4 szczeble × 3 biegi, label rho25/50/75/90). ${escapeHtml(muTxt)}`);
        } else {
            setHint('');
        }
    }
})();
