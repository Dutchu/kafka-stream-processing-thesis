window.GLOSSARY = {
    config: { t: 'CONFIG', d: 'Konfiguracja klastra: 1b-rf1 (1 broker), 3b-rf1 (3 brokery bez replikacji), 3b-rf3 (3 brokery z replikacją RF=3).' },
    brokers: { t: 'Bootstrap', d: 'Liczba brokerów widzianych przez AdminClient. Spadek poniżej konfiguracji = problem z klastrem.' },
    mu: { t: 'μ (profil)', d: 'Pojemność klastra w msg/s, zmierzona w E4 (plateau drabinki P). Bez μ nie ma ρ ani predykcji Kingmana.' },
    tau_ack: { t: 'τ_ack', d: 'Mediana opóźnienia ACK przy niskim obciążeniu (E1). Służy w Kingmanie jako średni czas obsługi.' },
    tau_e2e: { t: 'τ_e2e', d: 'Mediana opóźnienia end-to-end (producent → konsument) przy niskim obciążeniu (E1).' },
    ca2: { t: 'c_a²', d: 'Współczynnik zmienności napływu (indeks dyspersji zliczeń w oknach 100 ms). 1.0 = ruch Poissona.' },
    cs2: { t: 'c_s²', d: 'Współczynnik zmienności obsługi (CoV² opóźnienia ACK). Rośnie z niestabilnością brokerów.' },
    source: { t: 'Źródło profilu', d: 'Skąd pochodzą parametry: DEFAULT (brak kalibracji), runId (from-run), calibration … (zakładka Kalibracja), PUT (ręcznie).' },
    run: { t: 'Bieg', d: 'Aktywny bieg (runId + czas) albo brak. Tylko jeden bieg naraz.' },
    lambda_leo: { t: 'λ (LEO)', d: 'Strumień wejściowy z ΔLogEndOffset (AdminClient, 1 Hz) — niezależny od konsumenta. Podstawa ρ.' },
    lambda_consumer: { t: 'λ (konsument)', d: 'Tempo odczytu konsumenta e2e. Rozjazd z λ (LEO) = lag / wolny konsument.' },
    rho: { t: 'ρ', d: 'Wykorzystanie λ/μ. >80% na czerwono: tam rosną kolejki (Kingman ∝ ρ/(1−ρ)). ≥100% = przeciążenie.' },
    wq: { t: 'W_q', d: 'Przewidywane oczekiwanie w kolejce: ρ/(1−ρ)·(c_a²+c_s²)/2·τ. Sercem predykcji.' },
    pred_ack: { t: 'L_pred ACK / obs', d: 'Przewidywane (τ_ack + W_q) vs zmierzone średnie ACK. Rozjazd = ε.' },
    err_ack: { t: 'ε ACK', d: 'Błąd predykcji ACK: obs − pred (ms) i względny (%). Cel E2: mały na każdym ρ.' },
    pred_e2e: { t: 'L_pred e2e / obs', d: 'Przewidywane (τ_e2e + W_q) vs zmierzone średnie e2e (zegary Google NTP).' },
    err_e2e: { t: 'ε e2e', d: 'Błąd predykcji e2e, jak wyżej. e2e zawiera sieć i konsumenta, więc zwykle większe niż ε ACK.' },
    mae: { t: 'MAE 60 s', d: 'Średni błąd bezwzględny z ostatnich 60 s (ACK / e2e). Stabilność predykcji w czasie.' },
    lag: { t: 'Lag konsumenta', d: 'Σ(endOffsets − position). Lag > 5·λ przez >10% okna = flaga consumerLimited (e2e niewiarygodne).' },
    errors: { t: 'Błędy producenta', d: 'Klasyfikacja wyjątków funkcji (TimeoutException, BufferExhausted…). Pusto = czysto.' },
    total_msgs: { t: 'Σ wiadomości', d: 'Suma (LEO−LSO) po wszystkich partycjach — stan kolejki (całka). Pochodna to λ.' },
    field_exp: { t: 'Eksperyment', d: 'Wybór wstawia preset przepisu (E1/E4/E2) — pola przestawiają się same.' },
    field_rate: { t: 'Tempo', d: 'Limit msg/s na inwokację (0 = bez limitu, backpressure bufora). E4 zawsze 0.' },
    field_parallelism: { t: 'Równoległość (P)', d: 'Liczba równoległych inwokacji funkcji. Oś drabinki E4 (max 100 — limit Direct VPC).' },
    field_value: { t: 'Wartość trybu', d: 'Znaczenie zależy od trybu powyżej: sekundy albo liczba wiadomości.' },
    field_intensity: { t: 'Intensity', d: 'Stałe 0.7. Skaluje treść generatora (nie tempo); treść nie jest przedmiotem badania.' },
    chart_brokers: { t: 'Wiadomości w brokerach', d: 'Stan (LEO−LSO) per broker, słupki skumulowane: kolor = temat, pełny = lider, przezroczysty = follower.' },
    chart_leaders: { t: 'Liderzy', d: 'Jak wyżej, ale tylko partycje, których broker jest liderem + tabela partycja → lider/repliki/ISR.' },
    chart_kingman: { t: 'Kingman na żywo', d: 'L_pred vs obs (ACK/e2e, ostatnie 300 s) + ρ na drugiej osi. Źródło c²: profil albo live (Kalibracja).' },
    chart_epsilon: { t: 'ε na żywo', d: 'Błąd predykcji ACK/e2e w ms. Płasko przy zerze = Kingman trafia.' },
    chart_topics: { t: 'Tematy', d: 'Σ wiadomości per temat w klastrze — prosty widok ogólny.' },
    mode_profile: { t: 'Tryb profil', d: 'W_q z zamrożonych c² profilu. Porównywalne między biegami.' },
    mode_live: { t: 'Tryb live', d: 'W_q z bieżących estymat ticka (fallback na profil gdy NaN). Pokazuje, czy rozjazdy znikają.' }
};

window.attachGlossaryTooltips = function () {
    const byId = {
        'hdr-config': 'config', 'hdr-brokers': 'brokers', 'hdr-mu': 'mu',
        'hdr-tau-ack': 'tau_ack', 'hdr-tau-e2e': 'tau_e2e', 'hdr-ca2': 'ca2',
        'hdr-cs2': 'cs2', 'hdr-source': 'source', 'hdr-run': 'run',
        'val-lambda-leo': 'lambda_leo', 'val-lambda-consumer': 'lambda_consumer',
        'val-rho': 'rho', 'val-wq': 'wq', 'val-pred-ack': 'pred_ack',
        'val-err-ack': 'err_ack', 'val-pred-e2e': 'pred_e2e',
        'val-err-e2e': 'err_e2e', 'val-mae': 'mae', 'val-lag': 'lag',
        'val-errors': 'errors', 'val-total-msgs': 'total_msgs',
        'field-exp': 'field_exp', 'field-rate': 'field_rate',
        'field-parallelism': 'field_parallelism', 'field-value': 'field_value',
        'field-intensity': 'field_intensity',
        'chart-brokers': 'chart_brokers', 'chart-leaders': 'chart_leaders',
        'chart-kingman': 'chart_kingman', 'chart-epsilon': 'chart_epsilon',
        'chart-topics': 'chart_topics'
    };
    Object.entries(byId).forEach(([id, key]) => {
        let el = document.getElementById(id);
        const g = window.GLOSSARY[key];
        if (!el || !g) return;
        if (id.startsWith('chart-') && el.parentElement) el = el.parentElement;
        el.setAttribute('data-tip', `${g.t}: ${g.d}`);
    });
    const paramKey = { tauAckMs: 'tau_ack', tauE2eMs: 'tau_e2e', cs2: 'cs2', ca2: 'ca2', muMsgs: 'mu' };
    document.querySelectorAll('.calib-card[data-param]').forEach(card => {
        const g = window.GLOSSARY[paramKey[card.dataset.param]];
        const hint = card.querySelector('p.hint');
        if (g && hint) hint.innerText = g.d;
    });
};
document.addEventListener('DOMContentLoaded', () => window.attachGlossaryTooltips());
