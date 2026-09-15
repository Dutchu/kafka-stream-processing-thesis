(function () {
    'use strict';

    window.updateCharts = function () {};
    window.renderPartitionTable = function () {};

    const TOPIC_COLORS = {
        'weather-rain': '#3b82f6',
        'weather-temp': '#f59e0b',
        'weather-wind': '#10b981'
    };
    const DEFAULT_COLOR = '#94a3b8';

    function topicColor(topic) {
        return TOPIC_COLORS[topic] || DEFAULT_COLOR;
    }

    function withAlpha(hex, alpha) {
        const h = hex.replace('#', '');
        const r = parseInt(h.substring(0, 2), 16);
        const g = parseInt(h.substring(2, 4), 16);
        const b = parseInt(h.substring(4, 6), 16);
        return `rgba(${r}, ${g}, ${b}, ${alpha})`;
    }

    function num(v, fallback) {
        const n = Number(v);
        return Number.isFinite(n) ? n : (fallback === undefined ? 0 : fallback);
    }

    if (typeof Chart === 'undefined') {
        console.warn('charts: Chart.js not loaded (CDN unreachable?) — all charts disabled.');
        return;
    }

    function getCanvas(id) {
        const el = document.getElementById(id);
        if (!el || typeof el.getContext !== 'function') {
            console.warn('charts: canvas #' + id + ' not found — disabled.');
            return null;
        }
        return el;
    }

    const brokersCanvas = getCanvas('chart-brokers');
    let brokersChart = null;
    if (brokersCanvas) {
        brokersChart = new Chart(brokersCanvas.getContext('2d'), {
            type: 'bar',
            data: { labels: [], datasets: [] },
            options: {
                responsive: true, maintainAspectRatio: false, animation: { duration: 0 },
                scales: {
                    x: { stacked: true, grid: { color: '#333' }, ticks: { color: '#ccc' } },
                    y: { stacked: true, beginAtZero: true, grid: { color: '#333' }, ticks: { color: '#ccc' } }
                },
                plugins: {
                    legend: { labels: { color: '#e0e0e0', boxWidth: 12, font: { size: 10 } } },
                    tooltip: {
                        callbacks: {
                            afterLabel: (ctx) => {
                                const parts = ctx.dataset._partitions || {};
                                const n = parts[ctx.label];
                                return n !== undefined ? `partycje: ${n}` : '';
                            }
                        }
                    }
                }
            }
        });
    }

    const leadersCanvas = getCanvas('chart-leaders');
    let leadersChart = null;
    if (leadersCanvas) {
        leadersChart = new Chart(leadersCanvas.getContext('2d'), {
            type: 'bar',
            data: { labels: [], datasets: [] },
            options: {
                responsive: true, maintainAspectRatio: false, animation: { duration: 0 },
                scales: {
                    x: { stacked: true, grid: { color: '#333' }, ticks: { color: '#ccc' } },
                    y: { stacked: true, beginAtZero: true, grid: { color: '#333' }, ticks: { color: '#ccc' } }
                },
                plugins: { legend: { labels: { color: '#e0e0e0', boxWidth: 12, font: { size: 10 } } } }
            }
        });
    }

    function buildBrokerDatasets(cluster, leadersOnly) {
        const brokers = (cluster && Array.isArray(cluster.brokers)) ? cluster.brokers : [];
        const perBroker = (cluster && Array.isArray(cluster.perBroker)) ? cluster.perBroker : [];
        const labels = brokers.map(b => 'broker-' + b.brokerId);

        const topics = new Set();
        perBroker.forEach(pb => {
            const byTopic = pb.byTopic || {};
            Object.keys(byTopic).forEach(t => topics.add(t));
        });
        const topicList = Array.from(topics).sort();

        const datasets = [];
        topicList.forEach(topic => {
            const color = topicColor(topic);
            const leaderData = [];
            const leaderParts = {};
            const followerData = [];
            const followerParts = {};

            brokers.forEach(b => {
                const pb = perBroker.find(x => x.brokerId === b.brokerId);
                const role = pb && pb.byTopic ? pb.byTopic[topic] : null;
                const label = 'broker-' + b.brokerId;
                leaderData.push(role ? num(role.leaderMsgs) : 0);
                leaderParts[label] = role ? role.leaderPartitions : 0;
                followerData.push(role ? num(role.followerMsgs) : 0);
                followerParts[label] = role ? role.replicaPartitions : 0;
            });

            datasets.push({
                label: topic + ' (lider)',
                backgroundColor: color,
                data: leaderData,
                stack: 'stack0',
                _partitions: leaderParts
            });

            if (!leadersOnly) {
                datasets.push({
                    label: topic + ' (replika)',
                    backgroundColor: withAlpha(color, 0.4),
                    borderColor: color,
                    borderWidth: 1,
                    data: followerData,
                    stack: 'stack0',
                    _partitions: followerParts
                });
            }
        });

        return { labels, datasets };
    }

    function updateBrokersChart(tick) {
        if (!brokersChart) return;
        try {
            const built = buildBrokerDatasets(tick.cluster, false);
            brokersChart.data.labels = built.labels;
            brokersChart.data.datasets = built.datasets;
            brokersChart.update();
        } catch (err) {
            console.warn('charts: brokers chart update skipped —', err);
        }
    }

    function updateLeadersChart(tick) {
        if (!leadersChart) return;
        try {
            const built = buildBrokerDatasets(tick.cluster, true);
            leadersChart.data.labels = built.labels;
            leadersChart.data.datasets = built.datasets;
            leadersChart.update();
        } catch (err) {
            console.warn('charts: leaders chart update skipped —', err);
        }
    }

    window.renderPartitionTable = function (tick) {
        const tbody = document.querySelector('#table-partitions tbody');
        if (!tbody) return;
        try {
            const cluster = tick.cluster || {};
            const partitions = Array.isArray(cluster.partitions) ? cluster.partitions.slice(0, 18) : [];
            if (partitions.length === 0) {
                tbody.innerHTML = '<tr><td colspan="5" class="hint">brak danych</td></tr>';
                return;
            }
            tbody.innerHTML = partitions.map(p => {
                const replicas = Array.isArray(p.replicas) ? p.replicas.join(', ') : '';
                const isr = Array.isArray(p.isr) ? p.isr.join(', ') : '';
                return `<tr><td>${escapeHtml(p.topic)}</td><td>${p.partition}</td><td>${p.leader}</td>` +
                       `<td>${escapeHtml(replicas)}</td><td>${escapeHtml(isr)}</td></tr>`;
            }).join('');
        } catch (err) {
            console.warn('charts: partition table update skipped —', err);
        }
    };

    function escapeHtml(s) {
        return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({
            '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
        }[c]));
    }

    const MAX_POINTS = 300;
    const kingmanCanvas = getCanvas('chart-kingman');
    let kingmanChart = null;
    if (kingmanCanvas) {
        kingmanChart = new Chart(kingmanCanvas.getContext('2d'), {
            type: 'line',
            data: {
                labels: [],
                datasets: [
                    { label: 'L_pred ACK', borderColor: '#3b82f6', backgroundColor: 'transparent', data: [], yAxisID: 'y', tension: 0.2, pointRadius: 0 },
                    { label: 'obs ACK mean', borderColor: '#93c5fd', backgroundColor: 'transparent', data: [], yAxisID: 'y', tension: 0.2, pointRadius: 0, borderDash: [4, 3] },
                    { label: 'L_pred e2e', borderColor: '#f59e0b', backgroundColor: 'transparent', data: [], yAxisID: 'y', tension: 0.2, pointRadius: 0 },
                    { label: 'obs e2e mean', borderColor: '#fcd34d', backgroundColor: 'transparent', data: [], yAxisID: 'y', tension: 0.2, pointRadius: 0, borderDash: [4, 3] },
                    { label: 'rho', borderColor: '#10b981', backgroundColor: 'transparent', data: [], yAxisID: 'y1', tension: 0.2, pointRadius: 0 },
                    { label: '+MAE ACK', borderColor: 'rgba(59,130,246,0.25)', backgroundColor: 'transparent', data: [], yAxisID: 'y', tension: 0.2, pointRadius: 0, borderWidth: 1 },
                    { label: '±MAE ACK', borderColor: 'rgba(59,130,246,0.25)', backgroundColor: 'rgba(59,130,246,0.12)', data: [], yAxisID: 'y', tension: 0.2, pointRadius: 0, borderWidth: 1, fill: '-1' },
                    { label: '+MAE e2e', borderColor: 'rgba(245,158,11,0.25)', backgroundColor: 'transparent', data: [], yAxisID: 'y', tension: 0.2, pointRadius: 0, borderWidth: 1 },
                    { label: '±MAE e2e', borderColor: 'rgba(245,158,11,0.25)', backgroundColor: 'rgba(245,158,11,0.12)', data: [], yAxisID: 'y', tension: 0.2, pointRadius: 0, borderWidth: 1, fill: '-1' }
                ]
            },
            options: {
                responsive: true, maintainAspectRatio: false, animation: { duration: 0 },
                scales: {
                    x: { display: false },
                    y: { type: 'linear', position: 'left', title: { display: true, text: 'ms', color: '#888' }, grid: { color: '#333' }, ticks: { color: '#ccc' } },
                    y1: { type: 'linear', position: 'right', min: 0, max: 1, title: { display: true, text: 'rho', color: '#888' }, grid: { drawOnChartArea: false }, ticks: { color: '#ccc' } }
                },
                plugins: { legend: { labels: { color: '#e0e0e0', boxWidth: 12, font: { size: 10 } } } }
            }
        });
    }

    const epsilonCanvas = getCanvas('chart-epsilon');
    let epsilonChart = null;
    if (epsilonCanvas) {
        epsilonChart = new Chart(epsilonCanvas.getContext('2d'), {
            type: 'line',
            data: {
                labels: [],
                datasets: [
                    { label: 'epsilon ACK (ms)', borderColor: '#ef4444', backgroundColor: 'transparent', data: [], tension: 0.2, pointRadius: 0 },
                    { label: 'epsilon e2e (ms)', borderColor: '#f97316', backgroundColor: 'transparent', data: [], tension: 0.2, pointRadius: 0 }
                ]
            },
            options: {
                responsive: true, maintainAspectRatio: false, animation: { duration: 0 },
                scales: {
                    x: { display: false },
                    y: { grid: { color: '#333' }, ticks: { color: '#ccc' } }
                },
                plugins: { legend: { labels: { color: '#e0e0e0', boxWidth: 12, font: { size: 10 } } } }
            }
        });
    }

    function pushValue(dataset, value) {
        dataset.push(value);
        while (dataset.length > MAX_POINTS) dataset.shift();
    }

    function pushLabel(labels, label) {
        labels.push(label);
        while (labels.length > MAX_POINTS) labels.shift();
    }

    function nullable(v) {
        const n = Number(v);
        return Number.isFinite(n) ? n : null;
    }

    const maeAckHist = [];
    const maeE2eHist = [];

    function mean(a) {
        if (!a.length) return NaN;
        let s = 0;
        for (const v of a) s += v;
        return s / a.length;
    }

    function band(pred, mae, sign) {
        if (pred == null || !Number.isFinite(mae)) return null;
        return pred + sign * mae;
    }

    function updateKingmanCharts(tick) {
        const label = new Date(num(tick.tsMs)).toLocaleTimeString();
        const ea = Math.abs(num(tick.errAckMs));
        const ee = Math.abs(num(tick.errE2eMs));
        if (Number.isFinite(ea)) { maeAckHist.push(ea); while (maeAckHist.length > 60) maeAckHist.shift(); }
        if (Number.isFinite(ee)) { maeE2eHist.push(ee); while (maeE2eHist.length > 60) maeE2eHist.shift(); }
        const maeAck = mean(maeAckHist);
        const maeE2e = mean(maeE2eHist);
        if (kingmanChart) {
            try {
                pushLabel(kingmanChart.data.labels, label);
                const pA = nullable(tick.predAckMs);
                const pE = nullable(tick.predE2eMs);
                pushValue(kingmanChart.data.datasets[0].data, pA);
                pushValue(kingmanChart.data.datasets[1].data, nullable(tick.ackMeanMs));
                pushValue(kingmanChart.data.datasets[2].data, pE);
                pushValue(kingmanChart.data.datasets[3].data, nullable(tick.e2eMeanMs));
                pushValue(kingmanChart.data.datasets[4].data, nullable(tick.rho));
                pushValue(kingmanChart.data.datasets[5].data, band(pA, maeAck, 1));
                pushValue(kingmanChart.data.datasets[6].data, band(pA, maeAck, -1));
                pushValue(kingmanChart.data.datasets[7].data, band(pE, maeE2e, 1));
                pushValue(kingmanChart.data.datasets[8].data, band(pE, maeE2e, -1));
                kingmanChart.update();
            } catch (err) {
                console.warn('charts: kingman chart update skipped —', err);
            }
        }
        if (epsilonChart) {
            try {
                pushLabel(epsilonChart.data.labels, label);
                pushValue(epsilonChart.data.datasets[0].data, nullable(tick.errAckMs));
                pushValue(epsilonChart.data.datasets[1].data, nullable(tick.errE2eMs));
                epsilonChart.update();
            } catch (err) {
                console.warn('charts: epsilon chart update skipped —', err);
            }
        }
    }

    const topicsCanvas = getCanvas('chart-topics');
    let topicsChart = null;
    if (topicsCanvas) {
        topicsChart = new Chart(topicsCanvas.getContext('2d'), {
            type: 'bar',
            data: { labels: ['klaster'], datasets: [] },
            options: {
                indexAxis: 'y',
                responsive: true, maintainAspectRatio: false, animation: { duration: 0 },
                scales: {
                    x: { stacked: true, beginAtZero: true, grid: { color: '#333' }, ticks: { color: '#ccc' } },
                    y: { stacked: true, grid: { color: '#333' }, ticks: { color: '#ccc' } }
                },
                plugins: { legend: { labels: { color: '#e0e0e0', boxWidth: 12, font: { size: 10 } } } }
            }
        });
    }

    function updateTopicsChart(tick) {
        if (!topicsChart) return;
        try {
            const cluster = tick.cluster || {};
            const partitions = Array.isArray(cluster.partitions) ? cluster.partitions : [];
            const perTopic = {};
            partitions.forEach(p => {
                perTopic[p.topic] = (perTopic[p.topic] || 0) + num(p.msgs !== undefined ? p.msgs : (p.latest - p.earliest));
            });
            const topicList = Object.keys(perTopic).sort();
            topicsChart.data.datasets = topicList.map(topic => ({
                label: topic,
                backgroundColor: topicColor(topic),
                data: [perTopic[topic]]
            }));
            topicsChart.update();
        } catch (err) {
            console.warn('charts: topics chart update skipped —', err);
        }
    }

    const BROKER_COLORS = ['#3b82f6', '#f59e0b', '#10b981', '#ef4444', '#a78bfa'];
    const brokersTsCanvas = getCanvas('chart-brokers-ts');
    let brokersTsChart = null;
    if (brokersTsCanvas) {
        brokersTsChart = new Chart(brokersTsCanvas.getContext('2d'), {
            type: 'line',
            data: { labels: [], datasets: [] },
            options: {
                responsive: true, maintainAspectRatio: false, animation: { duration: 0 },
                scales: {
                    x: { display: false },
                    y: { type: 'linear', position: 'left', title: { display: true, text: 'msg/s (leader)', color: '#888' }, grid: { color: '#333' }, ticks: { color: '#ccc' } }
                },
                plugins: { legend: { labels: { color: '#e0e0e0', boxWidth: 12, font: { size: 10 } } } }
            }
        });
    }

    let prevBrokerTs = null;

    function brokerLeaderTotals(cluster) {
        const totals = {};
        const perBroker = (cluster && Array.isArray(cluster.perBroker)) ? cluster.perBroker : [];
        perBroker.forEach(pb => {
            let sum = 0;
            const byTopic = pb.byTopic || {};
            Object.keys(byTopic).forEach(t => { sum += num(byTopic[t].leaderMsgs); });
            totals[pb.brokerId] = sum;
        });
        return totals;
    }

    function updateBrokersTsChart(tick) {
        if (!brokersTsChart) return;
        try {
            const tsMs = num(tick.tsMs, NaN);
            const totals = brokerLeaderTotals(tick.cluster);
            if (prevBrokerTs && Number.isFinite(tsMs) && tsMs > prevBrokerTs.tsMs) {
                const dt = (tsMs - prevBrokerTs.tsMs) / 1000;
                if (dt > 0 && dt < 10) {
                    const label = new Date(tsMs).toLocaleTimeString();
                    pushLabel(brokersTsChart.data.labels, label);
                    Object.keys(totals).sort().forEach((id, i) => {
                        let ds = brokersTsChart.data.datasets.find(d => d._brokerId === id);
                        if (!ds) {
                            ds = {
                                _brokerId: id,
                                label: 'broker-' + id,
                                borderColor: BROKER_COLORS[i % BROKER_COLORS.length],
                                backgroundColor: 'transparent',
                                data: [], tension: 0.2, pointRadius: 0
                            };
                            for (let k = 0; k < brokersTsChart.data.labels.length - 1; k++) ds.data.push(null);
                            brokersTsChart.data.datasets.push(ds);
                        }
                        const prev = prevBrokerTs.totals[id];
                        const rate = (prev === undefined) ? null : Math.max(0, (totals[id] - prev) / dt);
                        pushValue(ds.data, rate);
                    });
                    brokersTsChart.update();
                }
            }
            prevBrokerTs = { tsMs, totals };
        } catch (err) {
            console.warn('charts: brokers-ts update skipped —', err);
        }
    }

    function baseUrl(path) {
        return (typeof window.dashUrl === 'function') ? window.dashUrl(path) : path;
    }

    async function refreshTopicsTable() {
        const tbody = document.querySelector('#table-topics tbody');
        if (!tbody) return;
        tbody.innerHTML = '<tr><td colspan="6" class="hint">ładowanie…</td></tr>';
        try {
            const [resp, watchedResp, shapeResp] = await Promise.all([
                fetch(baseUrl('/api/topics')),
                fetch(baseUrl('/api/topics/watched')).catch(() => null),
                fetch(baseUrl('/api/shape')).catch(() => null)
            ]);
            const rows = await resp.json();
            let watched = null;
            try {
                const w = watchedResp ? await watchedResp.json() : null;
                watched = w && Array.isArray(w.watched) && w.watched.length > 0 ? w.watched : null;
            } catch (_) {                            }
            const note = document.getElementById('shape-note');
            try {
                const s = shapeResp ? await shapeResp.json() : null;
                if (note && s) {
                    const match = (s.suggestedConfig === s.profileConfig) ? '✓' : '⚠ rozjazd!';
                    note.innerText = `kształt: ${s.suggestedConfig} (brokerów: ${s.brokers}), profil: ${s.profileConfig} ${match}`;
                }
            } catch (_) {                  }
            if (!Array.isArray(rows) || rows.length === 0) {
                tbody.innerHTML = '<tr><td colspan="6" class="hint">pusto</td></tr>';
                return;
            }
            tbody.innerHTML = rows.map(r => {
                if (r.error) {
                    return `<tr><td>${escapeHtml(r.name || '?')}</td><td colspan="5" class="hint">${escapeHtml(r.error)}</td></tr>`;
                }
                const leaders = r.leaders && typeof r.leaders === 'object'
                    ? Object.entries(r.leaders).map(([b, c]) => `broker ${b} (${c} part.)`).join(', ')
                    : '—';
                const msgs = (r.msgs === undefined || r.msgs === null) ? '—' : Number(r.msgs).toLocaleString('en-US');
                const checked = (watched === null || watched.includes(r.name)) ? ' checked' : '';
                const sys = (r.name || '').startsWith('__');
                const acts = sys
                    ? '<span class="hint">systemowy</span>'
                    : `<button type="button" data-purge="${escapeHtml(r.name)}" class="secondary">Wyczyść</button> ` +
                      `<button type="button" data-drop="${escapeHtml(r.name)}" class="secondary">Usuń</button>`;
                return `<tr><td><label><input type="checkbox" data-topic="${escapeHtml(r.name)}"${checked}> ${escapeHtml(r.name)}</label></td>` +
                    `<td>${escapeHtml(r.partitions)}</td>` +
                    `<td>${escapeHtml(r.rf)}</td><td>${msgs}</td><td>${escapeHtml(leaders)}</td><td>${acts}</td></tr>`;
            }).join('');
            tbody.querySelectorAll('input[type="checkbox"][data-topic]').forEach(cb => {
                cb.addEventListener('change', saveWatchedTopics);
            });
            tbody.querySelectorAll('button[data-purge]').forEach(btn => {
                btn.addEventListener('click', () => purgeTopic(btn.getAttribute('data-purge')));
            });
            tbody.querySelectorAll('button[data-drop]').forEach(btn => {
                btn.addEventListener('click', () => dropTopic(btn.getAttribute('data-drop')));
            });
        } catch (err) {
            tbody.innerHTML = `<tr><td colspan="6" class="hint">błąd: ${escapeHtml(err.message)}</td></tr>`;
        }
    }
    window.refreshTopicsTable = refreshTopicsTable;

    async function purgeTopic(name) {
        const note = document.getElementById('shape-note');
        if (!confirm(`Wyczyścić WSZYSTKIE wiadomości z tematu ${name}? Temat zostaje (partycje/RF/config bez zmian).`)) return;
        try {
            const resp = await fetch(baseUrl('/api/topics/' + encodeURIComponent(name) + '/purge'), { method: 'POST' });
            const res = await resp.json();
            if (note) note.innerText = (res.status === 'ok')
                ? `wyczyszczono ${name}.`
                : `błąd czyszczenia ${name}: ${res.message || '?'}`;
            refreshTopicsTable();
        } catch (err) {
            if (note) note.innerText = `błąd czyszczenia ${name}: ` + err.message;
        }
    }

    async function dropTopic(name) {
        const note = document.getElementById('shape-note');
        if (!confirm(`USUNĄĆ cały temat ${name} (metadata + dane)? Odtworzysz go skryptem create_topics.sh.`)) return;
        try {
            const resp = await fetch(baseUrl('/api/topics/' + encodeURIComponent(name)), { method: 'DELETE' });
            const res = await resp.json();
            if (note) note.innerText = (res.status === 'ok')
                ? `usunięto temat ${name}.`
                : `błąd usuwania ${name}: ${res.message || '?'}`;
            refreshTopicsTable();
        } catch (err) {
            if (note) note.innerText = `błąd usuwania ${name}: ` + err.message;
        }
    }

    async function saveWatchedTopics() {        const boxes = document.querySelectorAll('#table-topics input[type="checkbox"][data-topic]');
        const names = [...boxes].filter(b => b.checked).map(b => b.getAttribute('data-topic'));
        const note = document.getElementById('shape-note');
        try {
            await fetch(baseUrl('/api/topics/watched'), {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ topics: names })
            });
            if (note) note.innerText = `obserwowane (${names.length}): ${names.join(', ') || '—'}`;
        } catch (err) {
            if (note) note.innerText = 'błąd zapisu wyboru: ' + err.message;
        }
    }

    document.addEventListener('DOMContentLoaded', () => {
        const btn = document.getElementById('btn-refresh-topics');
        if (btn) btn.addEventListener('click', refreshTopicsTable);
    });

    window.updateCharts = function (tick) {
        if (!tick || typeof tick !== 'object') return;
        updateBrokersChart(tick);
        updateBrokersTsChart(tick);
        updateLeadersChart(tick);
        window.renderPartitionTable(tick);
        updateKingmanCharts(tick);
        updateTopicsChart(tick);
    };
})();
