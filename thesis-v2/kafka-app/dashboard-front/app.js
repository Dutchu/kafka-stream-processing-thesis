(function () {
    'use strict';

    function setText(id, text) {
        const el = document.getElementById(id);
        if (el) el.innerText = text;
    }

    function num(value, fallback) {
        const n = Number(value);
        return Number.isFinite(n) ? n : (fallback === undefined ? NaN : fallback);
    }

    function fmt(value, digits, unit) {
        const n = Number(value);
        if (!Number.isFinite(n)) return '—';
        return n.toFixed(digits === undefined ? 2 : digits) + (unit || '');
    }

    function fmtPct(value, digits) {
        const n = Number(value);
        if (!Number.isFinite(n)) return '—%';
        return (n * 100).toFixed(digits === undefined ? 1 : digits) + '%';
    }

    function updateHeader(tick) {
        const profile = (tick.profile && typeof tick.profile === 'object') ? tick.profile : {};
        setText('hdr-config', profile.config || '—');
        setText('hdr-mu', Number.isFinite(Number(profile.muMsgs)) ? fmt(profile.muMsgs, 0, ' msg/s') : 'brak profilu');
        setText('hdr-tau-ack', Number.isFinite(Number(profile.tauAckMs)) ? fmt(profile.tauAckMs, 2, ' ms') : '—');
        setText('hdr-tau-e2e', Number.isFinite(Number(profile.tauE2eMs)) ? fmt(profile.tauE2eMs, 2, ' ms') : '—');
        setText('hdr-ca2', Number.isFinite(Number(profile.ca2)) ? fmt(profile.ca2, 3) : '—');
        setText('hdr-cs2', Number.isFinite(Number(profile.cs2)) ? fmt(profile.cs2, 3) : '—');
        setText('hdr-source', profile.source || 'DEFAULT');

        const run = (tick.run && typeof tick.run === 'object') ? tick.run : {};
        if (run.active) {
            setText('hdr-run', `${run.runId} (${run.elapsedSec}s)`);
        } else {
            setText('hdr-run', 'brak');
        }

        const cluster = (tick.cluster && typeof tick.cluster === 'object') ? tick.cluster : {};
        const brokerCount = Array.isArray(cluster.brokers) ? cluster.brokers.length : 0;
        setText('hdr-brokers', brokerCount + ' broker(y)');
    }

    function updateCards(tick) {
        setText('val-lambda-leo', fmt(tick.lambdaLeo, 1));
        setText('val-lambda-consumer', fmt(tick.lambdaConsumer, 1));

        const rho = num(tick.rho);
        setText('val-rho', Number.isFinite(rho) ? (rho * 100).toFixed(1) : '—');
        const rhoEl = document.getElementById('val-rho');
        if (rhoEl) {
            if (!Number.isFinite(rho)) rhoEl.style.color = '#888';
            else if (rho > 0.8) rhoEl.style.color = '#ef4444';
            else if (rho > 0.5) rhoEl.style.color = '#f59e0b';
            else rhoEl.style.color = '#3b82f6';
        }

        setText('val-wq', fmt(tick.wqMs, 2));
        setText('val-pred-ack', fmt(tick.predAckMs, 2));
        setText('val-obs-ack', 'obs ' + fmt(tick.ackMeanMs, 2));
        setText('val-err-ack', fmt(tick.errAckMs, 2));
        setText('val-err-ack-rel', fmtPct(tick.errAckRel));
        setText('val-pred-e2e', fmt(tick.predE2eMs, 2));
        setText('val-obs-e2e', 'obs ' + fmt(tick.e2eMeanMs, 2));
        setText('val-err-e2e', fmt(tick.errE2eMs, 2));
        setText('val-err-e2e-rel', fmtPct(tick.errE2eRel));
        setText('val-lag', String(Math.round(num(tick.consumerLag, 0))));
        setText('val-total-msgs', String(Math.round(num(tick.totalMsgsCluster, 0))));

        const errorTypes = typeof tick.errorTypes === 'string' ? tick.errorTypes : '';
        setText('val-errors', errorTypes.length > 0 ? errorTypes : 'brak');
    }

    function handleFrame(raw) {
        let data;
        try {
            data = JSON.parse(raw);
        } catch (err) {
            console.warn('app: dropping malformed WebSocket frame', err);
            return;
        }
        if (!data || typeof data !== 'object') return;

        try { updateHeader(data); } catch (err) { console.warn('app: header update failed', err); }
        try { updateCards(data); } catch (err) { console.warn('app: cards update failed', err); }

        try {
            if (typeof window.updateCharts === 'function') {
                window.updateCharts(data);
            }
        } catch (err) {
            console.warn('app: chart update failed', err);
        }

        try {
            if (typeof window.onTickForControl === 'function') {
                window.onTickForControl(data);
            }
        } catch (err) {
            console.warn('app: control panel update failed', err);
        }
    }

    let retryMs = 1000;

    function connect() {
        let ws;
        try {
            ws = new WebSocket(typeof window.dashWsUrl === 'function' ? window.dashWsUrl()
                : ((window.location.protocol === 'https:' ? 'wss' : 'ws') + '://' + window.location.host + '/ws'));
        } catch (err) {
            console.warn('app: WebSocket construction failed, retrying…', err);
            scheduleReconnect();
            return;
        }

        ws.onopen = () => {
            retryMs = 1000;
            console.log('WebSocket connected to /ws');
        };
        ws.onclose = () => {
            console.log('WebSocket disconnected — reconnecting in ' + retryMs + ' ms');
            scheduleReconnect();
        };
        ws.onerror = (err) => {
            console.warn('WebSocket error', err);
        };
        ws.onmessage = (event) => handleFrame(event.data);
    }

    function scheduleReconnect() {
        setTimeout(connect, retryMs);
        retryMs = Math.min(retryMs * 2, 15000);
    }

    function api(path) {
        return (typeof window.dashUrl === 'function') ? window.dashUrl(path) : path;
    }

    async function refreshProfileSelector() {
        const sel = document.getElementById('profile-select');
        if (!sel) return;
        try {
            const resp = await fetch(api('/api/profiles'));
            const data = await resp.json();
            const rows = (data && data.profiles) || [];
            const active = data && data.active;
            const keep = sel.value;
            sel.innerHTML = rows.map(r => {
                const prof = r.profile || {};
                const bits = [r.name, r.config, 'mu=' + fmtNum(prof.muMsgs)].join(' · ');
                return `<option value="${escAttr(r.name)}">${escHtml(bits)}</option>`;
            }).join('') || '<option value="">(pusta biblioteka)</option>';
            if (active && [...sel.options].some(o => o.value === active)) sel.value = active;
            else if (keep && [...sel.options].some(o => o.value === keep)) sel.value = keep;
            const note = document.getElementById('profile-active-note');
            if (note) note.innerText = active ? `aktywny: ${active}` : 'brak aktywnego';
        } catch (err) {
            console.warn('app: profile list failed', err);
        }
    }
    window.refreshProfileSelector = refreshProfileSelector;

    function escHtml(s) {
        return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({
            '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
        }[c]));
    }

    function escAttr(s) { return escHtml(s); }

    function fmtNum(v) {
        const n = Number(v);
        return Number.isFinite(n) ? (Math.abs(n) >= 100 ? Math.round(n) : n.toFixed(2)) : '—';
    }

    document.addEventListener('DOMContentLoaded', () => {
        const sel = document.getElementById('profile-select');
        if (sel) sel.addEventListener('change', async () => {
            if (!sel.value) return;
            try {
                await fetch(api('/api/profile/active'), {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ name: sel.value })
                });
                refreshProfileSelector();
            } catch (err) {
                console.warn('app: profile activation failed', err);
            }
        });
        refreshProfileSelector();
        setInterval(refreshProfileSelector, 15000);
    });

    connect();
})();
