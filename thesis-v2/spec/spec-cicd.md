# spec-cicd — gałęzie, GitHub Actions i metody aktualizacji

## 0. Zasada: repo jest źródłem prawdy (decyzja 2026-09-11)

- Kontroler (`kafka-monitoring:~/thesis-v2`) to **klon gałęzi `app-dist`** (`--single-branch`), nie śmietnik scp: build-playbook zaczyna od `git pull --ff-only` (nie przeszedł → ostrzeżenie + praca na lokalnym stanie, nigdy cicha ściema). `app-dist` niesie cały materiał wdrożeniowy (kod, playbooki, `config/`, `profiles.json`, `function_url.env`, CI) — haczyk syncuje go przy każdej zmianie wdrożeniowej.
- Auth persistuje w `~/.ssh/config` (host `github-thesis` + deploy-key, remote przepisany) — jednorazowy `GIT_SSH_COMMAND` działa tylko na klona, nie na pulle.
- Klonuj do pustego (kropka na końcu `git clone ... .`) — inaczej schodki `thesis-v2/thesis-v2`.
- Kształt klastra to **plik** `thesis-v2/config/<config>.env` w repo (CONFIG/KAFKA_SEED/TOPICS/RF/...); build-playbook kładzie wybrany jako `/opt/thesis-dashboard/cluster.env`, aplikacja czyta go przy starcie (jawny env procesowy wygrywa — tylko awaryjny override). Flagi `-e config=` wybierają już tylko KTÓRY plik, nie niosą wartości. Unit systemd NIE ma CONFIG/KAFKA_BROKERS/TOPICS (celowo — inaczej przykryłyby plik).

## 1. Gałęzie (po co każda istnieje)

| gałąź | zawartość | rola |
|---|---|---|
| `main` | wszystko (praca, wyniki, kod) | robocza; jedyne miejsce commitów autora |
| `app-dist` | `thesis-v2/kafka-app/**` + `.github/**` | kanał backendu: push odpala `dashboard-v2 CI/CD` |
| `app-front` | `thesis-v2/kafka-app/dashboard-front/**` + `.github/**` | kanał frontendu: push odpala `frontend hot-reload CI/CD` |
| `main-clean` | snapshot `main` bez binariów | czysta kopia w chmurze (snapshoty na kamienie milowe, nie CI) |

`app-dist`/`app-front` to sieroty (osobne historie) — NIGDY merge ani PR między nimi a `main`. `main-clean` też sierota (odświeżana rytuałem, nie syncem).

## 2. Workflows (`.github/workflows/`)

Wspólne: IAP tunnel (`--tunnel-through-iap`, firewall `allow-ssh-iap`, zakres `35.235.240.0/20`), sekret **tylko** `GCP_SA_KEY`, zmienne `GCP_PROJECT/GCP_ZONE/SSH_USER`, `CLOUDSDK_PYTHON=/usr/bin/python3` + numpy (szybki tunel), cel `kafka-load`.

- **`deploy-dashboard.yml`** (`dashboard-v2 CI/CD`): trigger push na `app-dist` ze zmianą `dashboard/**`. Bramka `mvn -q test` (Java 17). Deploy: `package` fat-jara → stempel SHA do `version.txt` → `scp` jara + stempla do `/opt/thesis-deploy/inbox/` → KONIEC (fire-and-forget). Resztę robi agent na hoście (`thesis-deploy-agent` przez `systemd.path`: stop/ubój/podmiana/start/health-check/rollback, status w `/opt/thesis-deploy/status`). Zero `ssh --command`, zero sleepów w CI (decyzje).
- **`deploy-frontend.yml`** (`frontend hot-reload CI/CD`): trigger push na `app-front`. Bramka `node --check` na każdy JS. Deploy: stempel SHA → `scp` jawną listą plików (`"$PUBLIC_DIR"/*`, nie katalog ze slashem — gcloud klonuje katalog) do `/opt/thesis-frontend/` → weryfikacja `curl localhost:8080/version.txt | grep SHA` (jedyny dozwolony `ssh --command`: odczyt, nie zapis). Bez restartu usługi (Javalin czyta statyki z dysku).

## 3. Metody aktualizacji (kolejność prób)

1. **Haczyk `post-commit`** (domyślny): commit na `main` → routing po ścieżkach (front → `sync_app_front.sh`, Java/pom → `sync_app_dist.sh`) przez worktree w `/tmp/thesis-app*`. Cichy fallback: jak sync padnie, commit i tak wchodzi, a haczyk tylko ostrzega.
2. **Ręczny sync** (gdy haczyk milczy — np. commity z PyCharma nie wołają hooków):
   `bash thesis-v2/gcloud/sync_app_front.sh main` / `bash thesis-v2/gcloud/sync_app_dist.sh main`.
3. **Naprawa worktree** (gdy `/tmp` wyczyszczony, błąd `index.lock`): `git worktree repair <ścieżka> [ścieżka]`; jak nie pomoże: `git worktree remove --force` + sync odtworzy.
4. **`main-clean`** (tylko ręcznie, na kamienie milowe): rytuał orphan-snapshot z filtrem binariów (null-delimited! spacje w nazwach) — patrz historia sesji 2026-09-10.

## 4. Weryfikacja po deployu

`bash thesis-v2/gcloud/verify_deploy.sh <backend-sha|-> <frontend-sha|->` (SHA z runów Actions) czyta `/api/version` tunelem i mówi OK/DRYF per kanał. `unknown` = stempel nie istnieje (pół-deploy). Frontend: `curl .../version.txt` ma zwrócić SHA z runa.
