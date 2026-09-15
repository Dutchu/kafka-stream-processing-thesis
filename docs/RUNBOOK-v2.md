# RUNBOOK-v2 — thesis-v2: sesja pełna (Faza A → Faza B)

**Rola:** ten dokument czyta się **od góry do dołu jako jedna sesja**. Każda komenda ma oczekiwany
wynik i (gdzie potrzebne) sekcję troubleshooting. Autor jest jedynym aktorem z `gcloud`/SSH.

**Wymaga wcześniej ukończonych:** W2 (`thesis-v2/kafka-app/function/`), W3 (`thesis-v2/kafka-app/dashboard/`),
W4 (`thesis-v2/monitoring_node/`, `deploy_jmx_v2.yml`, `update_monitoring_v2.yml` — **już dostarczone**).

**Dwa różne miejsca wykonania komend w tej sesji (rozróżniane konsekwentnie w każdej sekcji):**
- **`gcloud ...` / `bash gcloud/*.sh` / `terraform ...`** — uruchamiane **lokalnie na stacji autora**
  (Windows, katalog roboczy `C:\thesis\thesis-v2\`), gdzie jest zainstalowany `gcloud` CLI z aktywną
  autoryzacją (§0.2). Skrypty `gcloud/*.sh` same nawiązują `gcloud compute ssh`/`scp` do VM w razie
  potrzeby — nie trzeba być zalogowanym na żadną VM, żeby je odpalić.
- **`ansible-playbook ...`** — uruchamiane **na kontrolerze `kafka-monitoring`** (przez
  `gcloud compute ssh kafka-monitoring`), z katalogu `~/thesis-v2/ansible/`, poprzedzone
  synchronizacją `thesis-v2/` na tę VM. Pełny układ katalogów i komenda synchronizacji — §0.1.

---

## 0. Preflight

### 0.1 Gdzie uruchamiać Ansible i jak wygląda układ katalogów na kontrolerze

Kontrolerem Ansible jest **`kafka-monitoring`** (tak jak w pracy pierwotnej — SSH do
`kafka_nodes`/`monitoring`/`load_generator` z jednej maszyny; brak polegania na Ansible lokalnym na
stacji autora). Praca pierwotna ma tam już `~/ansible/` i `~/monitoring_node/` **obok siebie** w
`$HOME` (`update_monitoring.yml` odwołuje się do `../monitoring_node/grafana/*` względem
`~/ansible/`). thesis-v2 **nie miesza się z tym układem** — cały katalog `thesis-v2/` jest
synchronizowany jako jedna, osobna poddrzewo `~/thesis-v2/` obok istniejącego `~/ansible/` /
`~/monitoring_node/`:

```
~/ (kafka-monitoring, $HOME)
├── ansible/              (praca pierwotna — NIETKNIĘTE)
├── monitoring_node/      (praca pierwotna — NIETKNIĘTE)
└── thesis-v2/
    ├── ansible/           <- inventory.ini (kopia), kraft_reconfigure.yml, deploy_dashboard_v2.yml,
    │                          deploy_jmx_v2.yml, update_monitoring_v2.yml (dostarczone przez W4)
    ├── monitoring_node/   <- grafana/thesis-v2-*.json (dostarczone przez W4)
    └── gcloud/            <- skrypty tego workstreamu
```

**Komenda synchronizacji preflight (uruchom ją PIERWSZĄ, przed jakąkolwiek komendą `ansible-playbook`
w tej sesji i po KAŻDEJ zmianie plików w `thesis-v2/ansible/`, `thesis-v2/monitoring_node/` lub
`thesis-v2/gcloud/`):**

```bash
gcloud compute scp --recurse thesis-v2 kafka-monitoring:~/ --zone europe-central2-a \
  --exclude '.git,results,paper'
```

Cały katalog `thesis-v2/` (nie tylko `ansible/`/`monitoring_node/`/`gcloud/`) trafia jako `~/thesis-v2/`
— `--exclude 'results,paper'` pomija katalogi wynikowe/manuskrypt, jeśli są już duże (opcjonalne, ale
zalecane po pierwszych biegach E1/E4/E2; bez `--exclude` komenda nadal działa, tylko wolniej).
`gcloud compute scp --recurse` nie ma natywnej flagi `--exclude` we wszystkich wersjach gcloud — jeśli
Twoja wersja ją odrzuci, pomiń `results/`/`paper/` ręcznie (`rsync -e "gcloud compute ssh ..." ...` albo
`tar`+`scp` z wykluczeniem, albo po prostu usuń `--exclude ...` i zaakceptuj wolniejszy transfer przy
pierwszej synchronizacji, gdy `results/`/`paper/` są jeszcze puste/małe).

Wszystkie komendy `ansible-playbook` w tym RUNBOOK-u są odtąd uruchamiane **na `kafka-monitoring`**,
z katalogu `~/thesis-v2/ansible/`, z inventory `-i inventory.ini` (kopia w `thesis-v2/ansible/inventory.ini`
zsynchronizowana wyżej — **nie** `../../ansible/inventory.ini`, ten byłby inventory pracy pierwotnej):

```bash
gcloud compute ssh kafka-monitoring --zone europe-central2-a
cd ~/thesis-v2/ansible
ansible-playbook -i inventory.ini <playbook>.yml [-e ...]
```

Ścieżki względne wewnątrz playbooków v2 zakładają dokładnie ten układ:
- `deploy_dashboard_v2.yml` — odwołania relatywne wewnątrz `~/thesis-v2/ansible/`
  (np. `../kafka-app/dashboard`, `../gcloud/function_url.env`, `../analysis/output/profiles.json`)
  trafiają do `~/thesis-v2/kafka-app/`, `~/thesis-v2/gcloud/`, `~/thesis-v2/analysis/` — czyli do
  tego samego zsynchronizowanego poddrzewa `~/thesis-v2/`. `kraft_reconfigure.yml` nie ma odwołań
  względnych do plików na kontrolerze (operuje wyłącznie zdalnie na `kafka_nodes` przez
  moduły `systemd`/`command`/`copy` z treścią inline), więc jego jedyna zależność od układu
  katalogów to samo `inventory.ini` obok niego. `deploy_jmx_v2.yml` (W4) także ma treść JMX
  inline (bez zewnętrznego pliku źródłowego) — jego jedyna zależność to `inventory.ini`.
- `update_monitoring_v2.yml` (dostarczony przez W4, **nie modyfikować, tylko wywoływać**) czyta z
  **dwóch** różnych baz jednocześnie: `../../monitoring_node/grafana/*` (3 kropki wstecz z
  `~/thesis-v2/ansible/` → `~/monitoring_node/` — pliki **pracy pierwotnej**, niezmienione, dashboardy
  `thesis-kafka-dashboard.json`/`thesis-e9-closeout.json`) oraz `../monitoring_node/grafana/thesis-v2-*.json`
  (`~/thesis-v2/monitoring_node/` — pliki **thesis-v2** od W4: `thesis-v2-cluster-live.json`,
  `thesis-v2-kingman.json`, `thesis-v2-saturation.json`). Obie ścieżki muszą istnieć na kontrolerze
  **przed** uruchomieniem tego playbooka — dokładnie to sprawdza jego preflight `assert` (§2 niżej).
  Innymi słowy: ten jeden playbook wymaga zarówno oryginalnego `~/ansible`+`~/monitoring_node`
  (nietkniętych z pracy pierwotnej, powinny tam już być z wcześniejszych sesji) jak i nowo
  zsynchronizowanego `~/thesis-v2/monitoring_node` — synchronizacja wyżej dostarcza tylko tę drugą
  część; jeśli `~/ansible`/`~/monitoring_node` (bez `thesis-v2/`) nigdy nie były tam zsynchronizowane,
  zrób to teraz jednorazowo (poza zakresem tej sesji, ale zależność twarda):
  ```bash
  gcloud compute scp --recurse ansible monitoring_node kafka-monitoring:~/ --zone europe-central2-a
  ```

### 0.2 `gcloud auth`

```bash
gcloud auth login
gcloud config set project <TWOJ_PROJECT_ID>
gcloud auth list
```
**Oczekiwany wynik:** aktywne konto oznaczone `*`, projekt zgodny z `terraform/variables.tf` (`project_id`).

### 0.3 Weryfikacja monitoringu (read-only, wzorzec z pracy pierwotnej — OK do użycia bez zmian)

```bash
bash C:/thesis/gcloud/verify_monitoring.sh
```
**Oczekiwany wynik:** tabela PASS dla wszystkich sprawdzeń (JMX brokerów, targety Prometheus,
zapytania instant). **Jeśli FAIL** — patrz §6.3 (ten sam skrypt jest read-only i bezpieczny do
ponownego użycia dla thesis-v2, bo nazwy metryk `kafka_jmx`/`node_exporter` się nie zmieniają;
nowe metryki thesis-v2 z §2 spec-monitoring nie są tu jeszcze sprawdzane — to celowe, robi to
`update_monitoring_v2.yml` niżej).

### 0.4 Synchronizacja zegarów (read-only)

Uruchamiane na kontrolerze `kafka-monitoring` (patrz §0.1); `check_clock_sync.yml` jest wzorcem
pracy pierwotnej (`~/ansible/check_clock_sync.yml`, nietknięty), ale w inwentarzu odwołuje się do
tych samych hostów co thesis-v2 — wygodniej użyć **kopii inventory thesis-v2**
(`~/thesis-v2/ansible/inventory.ini`), więc uruchom go stamtąd wskazując playbook pracy pierwotnej:

```bash
gcloud compute ssh kafka-monitoring --zone europe-central2-a
cd ~/thesis-v2/ansible
ansible-playbook -i inventory.ini ../../ansible/check_clock_sync.yml
```
**Oczekiwany wynik:** tabela `PASS` dla `kafka-1/2/3`, `kafka-monitoring`, `kafka-load`. `WARN`/`UNCERTAIN`
nie przerywa sesji (play zawsze kończy się kodem 0), ale **nie zaczynaj sesji z >5 s skew** —
zsynchronizuj dotknięty zegar ręcznie (`sudo systemctl restart systemd-timesyncd` na VM) i powtórz.

---

## 1. Terraform v2 — sieć serverless (R4)

```bash
cd thesis-v2/terraform
terraform init
terraform plan
```
**Oczekiwany wynik:** `Plan: 2 to add, 0 to change, 0 to destroy.` — dokładnie:
`google_compute_subnetwork.serverless` (kafka-serverless-subnet) i
`google_compute_firewall.allow_serverless_to_kafka`. Zero odwołań do zasobów istniejącego stanu
(oryginalny `C:\thesis\terraform\` state nie jest w ogóle czytany — tylko `data` GCP API call).

```bash
terraform apply
```
Potwierdź `yes`. **Oczekiwany wynik:** `Apply complete! Resources: 2 added, 0 changed, 0 destroyed.`

**⚠️ Krok ręczny wymagany przed R5 (patrz `terraform/README.md`):** sprawdź, czy VM-y brokerów mają
tag sieciowy `kafka` (wymagany przez `target_tags` reguły firewall):
```bash
gcloud compute instances describe kafka-1 --zone europe-central2-a --format='value(tags.items)'
```
Jeśli pusto/brak `kafka`, dodaj (poza Terraform — addytywnie, nie zaburza istniejącego stanu):
```bash
gcloud compute instances add-tags kafka-1 kafka-2 kafka-3 --tags=kafka --zone europe-central2-a
```

### Troubleshooting §1
- `terraform plan` pokazuje >2 zasoby lub próbuje coś zmienić/usunąć → sprawdź, czy przypadkiem nie
  uruchomiłeś `terraform` z katalogu `C:\thesis\terraform\` (oryginał) zamiast `thesis-v2/terraform/`.
- `data.google_compute_network` nie znajduje `kafka-thesis-vpc` → zła nazwa projektu
  (`gcloud config get-value project`) lub sieć nie istnieje jeszcze (uruchom najpierw oryginalny
  `C:\thesis\terraform\` — poza zakresem tej sesji, ale jest to zależność twarda).

---

## 2. Monitoring v2 (W4 — reguły JMX rozszerzone, dashboardy Grafany)

**`deploy_jmx_v2.yml` i `update_monitoring_v2.yml` są dostarczone przez workstream W4 — ten RUNBOOK
je wyłącznie WYWOŁUJE, nie tworzy/nie modyfikuje.** Uruchamiane na kontrolerze `kafka-monitoring`,
z katalogu `~/thesis-v2/ansible/` (patrz układ i komenda synchronizacji w §0.1):

```bash
gcloud compute ssh kafka-monitoring --zone europe-central2-a
cd ~/thesis-v2/ansible
ansible-playbook -i inventory.ini deploy_jmx_v2.yml
```
**Oczekiwany wynik:** kafka zrestartowana na wszystkich OSIĄGALNYCH węzłach (w sesji startowej —
przed Fazą A — wszystkie 3 VM żyją, więc restart obejmuje wszystkie 3). Brak błędów fatalnych.

```bash
ansible-playbook -i inventory.ini update_monitoring_v2.yml
```
**Oczekiwany wynik:** `prometheus.yml` zawiera 2 oryginalne joby + nowy `dashboard_v2`; 3 nowe
dashboardy Grafany (`thesis-v2-cluster-live`, `thesis-v2-kingman`, `thesis-v2-saturation`) obok
istniejących. Preflight tego playbooka **wymaga jednocześnie DWÓCH baz plików na kontrolerze**
(patrz §0.1 dla pełnego wyjaśnienia ścieżek):
- `~/monitoring_node/grafana/*` (praca pierwotna, adresowane z playbooka jako `../../monitoring_node/grafana/*`
  względem `~/thesis-v2/ansible/`) — musi być tam z wcześniejszych sesji pracy pierwotnej;
- `~/thesis-v2/monitoring_node/grafana/thesis-v2-*.json` (adresowane jako `../monitoring_node/grafana/thesis-v2-*.json`)
  — dostarczone przez synchronizację `gcloud compute scp --recurse thesis-v2 ...` z §0.1.

Jeśli którakolwiek z tych ścieżek nie istnieje na kontrolerze, `assert` FAILuje z czytelnym
komunikatem wskazującym brakującą ścieżkę **przed** jakąkolwiek zmianą (preflight, brak
side-effectów przy błędzie).

**Weryfikacja:** `curl -s http://10.0.0.5:9090/api/v1/targets | grep dashboard_v2` (po deployu
dashboardu w §3c) powinno pokazać `"health":"up"` (dashboard jeszcze nie działa na tym etapie —
target będzie `down` do czasu Fazy A krok 3c; to oczekiwane).

### Troubleshooting §2
- `assert` preflight FAILuje na brakującym pliku Grafany z `~/thesis-v2/monitoring_node/...` →
  powtórz komendę synchronizacji z §0.1 (`gcloud compute scp --recurse thesis-v2 kafka-monitoring:~/ ...`)
  — prawdopodobnie plik nie dotarł lub zmienił się od ostatniej synchronizacji.
- `assert` preflight FAILuje na brakującym pliku z `~/monitoring_node/...` (bez `thesis-v2/`) →
  to jest baza **pracy pierwotnej**, osobna od thesis-v2; zsynchronizuj ją raz (`gcloud compute scp
  --recurse ansible monitoring_node kafka-monitoring:~/ --zone europe-central2-a`) — patrz §0.1,
  ostatni blok.
- JMX exporter nie eksportuje nowych metryk (`kafka_log_logendoffset` itp. puste) → sprawdź, czy
  `deploy_jmx_v2.yml` faktycznie zrestartował kafkę (`journalctl -u kafka -n 50` na brokerze) —
  zmiana w `jmx_exporter.yaml` wymaga restartu procesu, bo agent JVM ładuje config raz przy starcie.

---

## 3. FAZA A — konfiguracja `1b-rf1` (1 broker, RF=1)

### 3a. Przełączenie kworum na 1 węzeł (R1)

VM kafka-2/kafka-3 **muszą jeszcze żyć** w tym kroku (playbook się do nich łączy, żeby je
zatrzymać/wyczyścić) — NIE zatrzymuj ich w GCP przed tym krokiem. Uruchamiane na kontrolerze
`kafka-monitoring`, z `~/thesis-v2/ansible/` (patrz układ katalogów §0.1):

```bash
gcloud compute ssh kafka-monitoring --zone europe-central2-a
cd ~/thesis-v2/ansible
ansible-playbook -i inventory.ini kraft_reconfigure.yml -e quorum_size=1 -e confirm_wipe=yes
```
**Oczekiwany wynik:**
- Preflight wypisuje plan (kroki 1–7).
- `kafka-metadata-quorum.sh --bootstrap-server 10.0.0.6:9092 describe --status` w kroku 7 pokazuje
  `LeaderId` ustawiony i **1 voter** (10.0.0.6).
- `/etc/kafka-thesis-v2.env` na kafka-1 zawiera `CLUSTER_ID=<uuid>` i `QUORUM_SIZE=1`.
- kafka-2/kafka-3: usługa `kafka` **zatrzymana i disabled** (playbook nigdy jej nie startuje w tym
  trybie — kryterium akceptacji §5 spec-infra.md).

### 3b. Zatrzymanie VM kafka-2/3 (R2)

```bash
bash gcloud/stop_brokers.sh 2 3
```
**Oczekiwany wynik:** `STATUS: TERMINATED` dla `kafka-2`, `kafka-3` w tabeli wypisanej na końcu.

### 3c. Utworzenie tematów `1b-rf1` (R3)

```bash
bash gcloud/create_topics.sh 1b-rf1
```
**Oczekiwany wynik:** 4 linie `OK: <topic> created (partitions=<p> RF=1)` (pierwsze uruchomienie) —
`weather-rain/temp/wind` (6 partycji) + `control-metrics` (1 partycja), wszystkie RF=1,
`min.insync.replicas=1`. Bootstrap użyty wewnętrznie: **wyłącznie `10.0.0.6:9092`**.

### 3d. Deploy funkcji v2 (R5)

```bash
bash gcloud/deploy_function_v2.sh 1b-rf1
```
**Oczekiwany wynik:** `Deployment complete` (gen2), następnie potwierdzenie Direct VPC egress
(`kafka-thesis-vpc` / `kafka-serverless-subnet`), na końcu `FUNCTION_URL zapisany do
gcloud/function_url.env: https://...`. **Kod wyjścia 2** = flaga `--network/--subnet/--vpc-egress`
nie wspierana → patrz komunikat fallback wypisany przez skrypt (`--vpc-connector kafka-conn-waw`);
zdecyduj ręcznie, czy go użyć (patrz §6.2).

### 3e. Deploy dashboardu v2 (R6)

Nadal na kontrolerze `kafka-monitoring`, `~/thesis-v2/ansible/` (patrz §0.1):

```bash
ansible-playbook -i inventory.ini deploy_dashboard_v2.yml -e config=1b-rf1
```
**Oczekiwany wynik:** Maven build OK, usługa `thesis-dashboard` `active (running)`, post-check
`GET /api/health` → `{"status":"ok",...}`.

### 3f. E1 → E4 → E2 dla `1b-rf1` (R8)

```bash
bash gcloud/run_experiment.sh E1 1b-rf1 --runs 3
bash gcloud/run_experiment.sh E4 1b-rf1 --runs 3
```
**Oczekiwany wynik E1:** 3 katalogi `results/E1/1b-rf1/run{1,2,3}/` z `run.json`,
`series.json`, `ack.json`, `prom/*.json` (15 plików), `config.env`, `window.env`.
**Oczekiwany wynik E4:** drabinka `P ∈ {10,25,50,100,200,300}` wykonana do momentu wykrycia
plateau+sygnatury (log w `results/logs/E4_1b-rf1_<data>.log` pokazuje `lambdaMean` i
`saturationSignature.fired` per P) LUB wszystkie 6 szczebli, jeśli sygnatura się nie zapaliła
(§5.4 experiment-plan.md — w takim wypadku μ **nie może być ogłoszone** bez eskalacji z §4 planu;
to decyzja autora, nie tego skryptu).

Po E4, **ręcznie** (poza zakresem tego RUNBOOK-u — należy do W5 pipeline):
```bash
# 1. python analysis/analyze.py --config 1b-rf1   (W5, poza zakresem W1)
#    -> generuje analysis/output/profiles.json (muMsgs dla 1b-rf1)
#    Jesli pipeline W5 dziala na stacji autora (nie na kafka-monitoring), po
#    wygenerowaniu profiles.json powtorz synchronizacje z §0.1 zanim uruchomisz
#    ponizszy redeploy (thesis-v2/analysis/output/profiles.json musi trafic do
#    ~/thesis-v2/analysis/... na kontrolerze, skad deploy_dashboard_v2.yml go czyta).
# 2. redeploy dashboardu (na kontrolerze kafka-monitoring, ~/thesis-v2/ansible/),
#    zeby wczytal nowy profil:
ansible-playbook -i inventory.ini deploy_dashboard_v2.yml -e config=1b-rf1
```
**Oczekiwany wynik:** dashboard po restarcie pokazuje profil `1b-rf1` (μ/τ/c²) w nagłówku UI zamiast
`DEFAULT`.

```bash
bash gcloud/run_experiment.sh E2 1b-rf1 --runs 3 --mu <MU_Z_PROFILES_JSON>
```
(`--mu` opcjonalne, jeśli `analysis/output/profiles.json` już ma `muMsgs` dla `1b-rf1` — skrypt go
odczyta automatycznie). **Oczekiwany wynik:** 4 podkatalogi `results/E2/1b-rf1/rho{25,50,75,90}/run{1,2,3}/`.

### Troubleshooting §3 (Faza A)
- **Kworum bez lidera** (`describe --status` nie pokazuje `LeaderId`) → sprawdź, czy `format` z kroku
  5 playbooka faktycznie się wykonał na kafka-1 (`ls /var/lib/kafka/data/meta.properties` na hoście);
  jeśli katalog pusty, ponów `kraft_reconfigure.yml quorum_size=1` od nowa (idempotentne — usunie
  puste dane i sformatuje ponownie).
- **`create_topics.sh` zwraca błąd RF mismatch** → temat istnieje z inną RF z poprzedniej sesji;
  uruchom `bash gcloud/cleanup_topics_v2.sh` i powtórz `create_topics.sh 1b-rf1`.
- **Funkcja bez dostępu do 9092** (błędy `TimeoutException`/`UNKNOWN_HOST` w odpowiedziach HTTP) →
  sprawdź firewall (`terraform plan` w §1 pokazuje regułę) i tag sieciowy `kafka` na kafka-1
  (§1 krok ręczny); zweryfikuj też, że Direct VPC egress faktycznie się skonfigurował
  (`gcloud run services describe hexweather-producer-v2 --region europe-central2` →
  pole `vpcAccess`).
- **Dashboard lag rośnie** (`consumerLimited=true` w summary) → host `kafka-load` (e2-standard-2)
  może być przeciążony przy wysokim P w E4; to oczekiwane w E4 wysokich P i jest dokumentowane jako
  `consumer_limited` (e2e niewiarygodne, ACK nadal wiarygodne — plan §5.2).

---

## 4. FAZA B — konfiguracje `3b-rf1` i `3b-rf3` (3 brokery)

### 4a. Uruchomienie VM kafka-2/3 (R2)

```bash
bash gcloud/start_brokers.sh 2 3
```
**Oczekiwany wynik:** SSH dostępny na obu VM po ≤ kilku próbach; `systemctl is-active kafka` może
pokazać `inactive`/`failed` (oczekiwane — usługa była `disabled` z Fazy A, patrz §3a) — to NIE jest
błąd tego skryptu.

### 4b. Przełączenie kworum na 3 węzły (R1)

Nadal na kontrolerze `kafka-monitoring`, `~/thesis-v2/ansible/` (patrz §0.1):

```bash
ansible-playbook -i inventory.ini kraft_reconfigure.yml -e quorum_size=3 -e confirm_wipe=yes
```
**Oczekiwany wynik:** analogicznie do §3a, ale `describe --status` pokazuje **3 votery**
(10.0.0.6, 10.0.0.3, 10.0.0.4); wszystkie 3 usługi `kafka` `active (running)` i `enabled`.

### 4c. Dla `3b-rf1`, następnie dla `3b-rf3`: tematy → funkcja → dashboard → E1/E4/E2

Poniższy blok wykonaj **raz dla `CONFIG=3b-rf1`, potem ponownie dla `CONFIG=3b-rf3`** (RF tematów
zmienia się między nimi — tematy muszą zostać usunięte i utworzone na nowo, Kafka nie pozwala
zmienić RF istniejącego tematu w miejscu). Komendy `gcloud/*.sh` — **lokalnie** (stacja autora,
`C:\thesis\thesis-v2\`); komendy `ansible-playbook` — **na kontrolerze `kafka-monitoring`**, z
`~/thesis-v2/ansible/` (patrz rozróżnienie miejsc wykonania na początku dokumentu i układ w §0.1):

```bash
# --- lokalnie (stacja autora) ---
CONFIG=3b-rf1   # potem: CONFIG=3b-rf3

bash gcloud/cleanup_topics_v2.sh          # no-op przy pierwszym przejsciu (brak tematow po Fazie A)
bash gcloud/create_topics.sh "$CONFIG"
bash gcloud/deploy_function_v2.sh "$CONFIG" --reconfigure-only   # KAFKA_BROKERS sie nie zmienia miedzy 3b-rf1/3b-rf3, ale jest ta sama lista 3 brokerow
```
```bash
# --- na kontrolerze kafka-monitoring, ~/thesis-v2/ansible/ ---
ansible-playbook -i inventory.ini deploy_dashboard_v2.yml -e config="$CONFIG"
```
```bash
# --- lokalnie (stacja autora) ---
bash gcloud/run_experiment.sh E1 "$CONFIG" --runs 3
bash gcloud/run_experiment.sh E4 "$CONFIG" --runs 3
# ... W5 pipeline -> profiles.json -> redeploy dashboard (jak w §3f) ...
```
```bash
# --- na kontrolerze kafka-monitoring, ~/thesis-v2/ansible/ ---
ansible-playbook -i inventory.ini deploy_dashboard_v2.yml -e config="$CONFIG"
```
```bash
# --- lokalnie (stacja autora) ---
bash gcloud/run_experiment.sh E2 "$CONFIG" --runs 3 --mu <MU_Z_PROFILES_JSON>
```

**Uwaga o `deploy_function_v2.sh --reconfigure-only`:** dla przejścia `3b-rf1 → 3b-rf3` KAFKA_BROKERS
jest identyczny (ta sama lista 3 adresów) — krok jest tu wypisany dla kompletności/spójności sesji,
ale w praktyce jest no-op na wartości env (nadal wykonuje się bezpiecznie, idempotentnie). Dla
pierwszego przejścia `1b-rf1 → 3b-rf1` **NIE używaj** `--reconfigure-only` (funkcja jeszcze nie
istnieje w tej sesji jeśli to pierwsze uruchomienie Fazy B — ale ponieważ w tej sesji funkcja
`hexweather-producer-v2` już istnieje z Fazy A, `--reconfigure-only` jest tu poprawnym wyborem,
bo zmienia tylko KAFKA_BROKERS bez przebudowy źródła).

**Oczekiwany wynik (każdy z dwóch przebiegów):** identyczny układ katalogów jak w Fazie A, pod
`results/{E1,E4,E2}/3b-rf1/...` i osobno `results/{E1,E4,E2}/3b-rf3/...`.

### Troubleshooting §4 (Faza B)
- **`create_topics.sh 3b-rf3` FAILuje z RF mismatch** → tematy z `3b-rf1` (RF=1) nadal istnieją;
  uruchom `cleanup_topics_v2.sh` (usuwa `weather-*`+`control-metrics`) przed ponownym `create_topics.sh`.
- **Kworum bez lidera po powrocie do 3** → identycznie jak §3 troubleshooting, ale sprawdź WSZYSTKIE
  3 węzły (`ls /var/lib/kafka/data/meta.properties` na każdym) — format musiał się wykonać na
  wszystkich trzech w trybie quorum_size=3.
- **Dashboard nadal pokazuje `KAFKA_BROKERS` z Fazy A** → `deploy_function_v2.sh --reconfigure-only`
  zmienia env FUNKCJI, nie dashboardu; dashboard czyta `KAFKA_BROKERS` z własnej usługi systemd
  (ustawianej przez `deploy_dashboard_v2.yml -e config=...`) — upewnij się, że redeploy dashboardu
  faktycznie przebiegł (`systemctl status thesis-dashboard`, sprawdź `Environment=` w
  `/etc/systemd/system/thesis-dashboard.service`).

---

## 5. Zakończenie sesji (teardown częściowy)

**Nie niszcz Terraform v2 do końca prac** (kolejne sesje mogą wymagać ponownego użycia sieci
serverless). Zatrzymaj tylko VM:

```bash
bash C:/thesis/gcloud/stop.sh
# zatrzymuje: kafka-1 kafka-2 kafka-3 kafka-monitoring kafka-load (wzorzec z pracy pierwotnej)
```
**Oczekiwany wynik:** wszystkie 5 VM `TERMINATED`. Cloud Function/Cloud Run pozostają wdrożone
(nie generują kosztu w stanie bezczynności poza ew. minimalnym `--min-instances`, tu nieustawionym
→ 0 domyślnie).

Na koniec **całości** prac (dopiero po zamknięciu manuskryptu, W6):
```bash
cd thesis-v2/terraform
terraform destroy
```

---

## 6. Troubleshooting — indeks ogólny

### 6.1 Kworum bez lidera (dowolna faza)
Objaw: `kafka-metadata-quorum.sh describe --status` wisi/timeout, albo brak `LeaderId`.
Diagnoza: `journalctl -u kafka -n 100` na węźle(ach) aktywnym(ych) — szukaj `Fatal error during KafkaServerStartable startup` lub `InconsistentClusterIdException`. Najczęstsza przyczyna: katalog danych
nie został w pełni wyczyszczony przed formatem (stary `meta.properties` z INNYM cluster-id).
Remediacja: powtórz `kraft_reconfigure.yml` z tym samym `quorum_size` — jest idempotentny (czyści
ponownie, generuje nowy cluster-id, formatuje ponownie).

### 6.2 Funkcja bez dostępu do 9092 (Direct VPC egress)
Objaw: wszystkie inwokacje funkcji zwracają `errors:{"TimeoutException":N}` w odpowiedzi HTTP.
Diagnoza:
```bash
gcloud run services describe hexweather-producer-v2 --region europe-central2 --format='value(spec.template.metadata.annotations)'
```
Szukaj adnotacji `run.googleapis.com/network-interfaces` lub `vpc-access-connector`. Jeśli brak →
Direct VPC egress się nie skonfigurował (krok 2 `deploy_function_v2.sh` zwrócił kod 2, sprawdź jego
log). Remediacja: albo ponów krok 2 ręcznie po aktualizacji `gcloud components update`, albo użyj
fallbacku wypisanego przez skrypt (`--vpc-connector kafka-conn-waw`) — **decyzja autora**, bo
współdzieli connector z funkcją v1.
Sprawdź też firewall/tag (§1 krok ręczny) — reguła `allow-serverless-to-kafka` wymaga tagu `kafka`
na VM brokerów.

### 6.3 Dashboard lag / brak danych w panelach
Objaw: `GET /api/health` OK, ale karty λ/ρ puste lub `NaN`.
Diagnoza: `curl -s localhost:8080/api/tick | head -c 500` na kafka-load — sprawdź pole `cluster`
(czy `brokers[]` niepuste — w trybie 1b-rf1 oczekiwany 1 broker, nie błąd). Jeśli `profile.muMsgs`
jest `NaN` — normalne przed pierwszym E4+`profiles.json` (UI ma pokazać "brak profilu — wykonaj
E1/E4", to nie jest błąd).
Jeśli metryki JMX (`kafka_log_logendoffset` itd.) brakują w Grafanie/`collect_run_artifacts.sh` →
uruchom ponownie §2 (`deploy_jmx_v2.yml` + `update_monitoring_v2.yml`, na kontrolerze `kafka-monitoring`,
`~/thesis-v2/ansible/` — patrz §0.1) i zweryfikuj `curl localhost:7071/metrics | grep kafka_log_logendoffset`
na dowolnym aktywnym brokerze.

### 6.4 `run_experiment.sh` — bieg oznaczony FAILED w logu
Sprawdź `results/logs/<EXP>_<CONFIG>_<data>.log` — linia `FAILED: ...` z konkretnym powodem
(POST /api/run bez runId, timeout `wait_finished`, lub status końcowy ≠ `finished`). Seria
**kontynuuje się mimo to** (kryterium §5 spec-infra.md) — po zakończeniu serii, ręcznie dogoń brakujący
bieg: `bash gcloud/run_experiment.sh <EXP> <CONFIG> --runs 1 --start <N>`.

### 6.5 `ansible-playbook` nie znajduje plików / `assert` preflight FAILuje na ścieżce względnej
Objaw: błąd w stylu "could not find file ../monitoring_node/..." albo "../kafka-app/dashboard nie istnieje".
Diagnoza: playbook uruchomiony z niewłaściwego katalogu (np. lokalnie z `C:\thesis\thesis-v2\ansible\`
zamiast na kontrolerze z `~/thesis-v2/ansible/`), albo `thesis-v2/` nie została (ponownie)
zsynchronizowana na `kafka-monitoring` po ostatniej zmianie plików. Remediacja: powtórz komendę
synchronizacji z §0.1 i upewnij się, że jesteś zalogowany na `kafka-monitoring`
(`gcloud compute ssh kafka-monitoring`) i w katalogu `~/thesis-v2/ansible/` przed uruchomieniem
`ansible-playbook -i inventory.ini ...`.

---

## Dodatek: mapa plików wywoływanych w tej sesji

| Krok RUNBOOK | Plik | Gdzie uruchamiane |
| --- | --- | --- |
| §0.1 | (synchronizacja `gcloud compute scp --recurse thesis-v2 ...`) | lokalnie (stacja autora) |
| §1 | `terraform/{provider.tf,variables.tf,main.tf}` | lokalnie |
| §2 | `ansible/{deploy_jmx_v2.yml,update_monitoring_v2.yml}` (W4, tylko wywoływane) | kontroler `kafka-monitoring`, `~/thesis-v2/ansible/` |
| §3a, §4b | `ansible/kraft_reconfigure.yml` | kontroler `kafka-monitoring`, `~/thesis-v2/ansible/` |
| §3b | `gcloud/stop_brokers.sh` | lokalnie |
| §3c, §4c | `gcloud/create_topics.sh`, `gcloud/cleanup_topics_v2.sh` | lokalnie |
| §3d, §4c | `gcloud/deploy_function_v2.sh` | lokalnie |
| §3e, §4c | `ansible/deploy_dashboard_v2.yml` | kontroler `kafka-monitoring`, `~/thesis-v2/ansible/` |
| §3f, §4c | `gcloud/run_experiment.sh` → `gcloud/collect_run_artifacts.sh` | lokalnie |
| §4a | `gcloud/start_brokers.sh` | lokalnie |
| wszystkie (lokalne) | `gcloud/cluster_hosts_v2.env` (sourced przez każdy `.sh`) | lokalnie |

**Zasada:** `ansible-playbook` → zawsze kontroler `kafka-monitoring` z `~/thesis-v2/ansible/`
(inventory `-i inventory.ini` = kopia w `thesis-v2/ansible/inventory.ini`, zsynchronizowana §0.1).
`gcloud`/`bash gcloud/*.sh`/`terraform` → zawsze lokalnie, stacja autora z aktywnym `gcloud auth` (§0.2).
