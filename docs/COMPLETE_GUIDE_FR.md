# NimbusChain Fetch — Guide Complet (FR)

Ce document est le **guide principal** pour comprendre et exploiter ce repo de bout en bout.

Il couvre:
- la logique du projet,
- l’architecture conteneurisée,
- le cycle de vie d’un job,
- l’usage UI + API + terminal,
- les tests,
- le troubleshooting des erreurs courantes.

---

## 1. Objectif du projet

NimbusChain Fetch sert à:
- rechercher des produits satellite (Copernicus / USGS),
- lancer des téléchargements asynchrones,
- suivre l’état des jobs en temps réel,
- stocker l’état et les résultats de manière persistante (MongoDB),
- exposer une UI Streamlit orientée opération.

En pratique, il remplace un ancien flux "CLI local + parsing de logs" par un flux robuste basé sur:
- API jobs (`/v1/jobs`),
- événements SSE (`/v1/events`),
- worker indépendant pour l’exécution.

---

## 2. Ce qui tourne réellement

Le mode recommandé est une stack 4 services:

1. `nimbus-mongodb`
   - base de données (état jobs, events, résultats).
2. `nimbus-api`
   - FastAPI (création/lecture/annulation jobs, SSE, health, metrics).
3. `nimbus-worker`
   - exécute les jobs (providers, downloads, checksums, manifest).
4. `nimbus-ui`
   - Streamlit (map, tiles, preview, lancement jobs, suivi, résultats).

Le projet est découplé: UI, API, worker, DB sont indépendants et communiquent via HTTP + MongoDB.

---

## 3. Architecture logique

```text
Browser (UI)
  -> nimbus-ui (Streamlit)
     -> nimbus-api (FastAPI)
        -> MongoDB (job state/events/results)
     -> nimbus-worker lit les jobs queued/running depuis MongoDB
        -> providers externes (Copernicus / USGS)
        -> fichiers dans /data/downloads
```

### Flux d’un job

1. UI envoie `POST /v1/jobs` (ou `/v1/jobs/batch`).
2. API crée le job en base: `state=queued`.
3. Worker claim le job (lock DB), passe en `running`.
4. Worker publie des events `job.progress` + met à jour bytes/progress.
5. En fin:
   - `succeeded` + résultat disponible via `/v1/jobs/{job_id}/result`,
   - ou `failed`,
   - ou `cancelled`.
6. UI écoute `/v1/events` (SSE) et fallback en polling `/v1/jobs/{job_id}`.

---

## 4. Legacy UI vs nouveau runtime

La UI actuelle conserve la logique UX legacy:
- Map AOI,
- système de tuiles,
- tile search/picker,
- panneau Download,
- onglets Results/Settings.

Mais l’exécution n’est plus un subprocess local (`nohup`, PID) côté UI.

Maintenant:
- `Start Download` -> jobs API,
- `Stop` -> cancel jobs actifs,
- `Reset` / `Unlock` -> reset état UI uniquement (ne supprime pas les fichiers).

---

## 5. Arborescence utile du repo

- `src/nimbuschain_fetch/`
  - cœur moteur (providers, downloader, models, orchestration).
- `src/nimbuschain_fetch_service/`
  - API FastAPI.
- `src/nimbuschain_fetch_ui/`
  - UI Streamlit.
- `podman-compose.yml`
  - orchestration locale recommandée.
- `docker-compose.yml`
  - équivalent docker.
- `scripts/10_up_stack.sh`
  - start stack Podman.
- `scripts/11_down_stack.sh`
  - stop stack Podman.
- `scripts/12_scale_workers.sh`
  - scale workers.
- `tests/`
  - tests unitaires.

---

## 6. Variables d’environnement (groupe par groupe)

Source des settings: `src/nimbuschain_fetch/settings.py`.

### 6.1 DB / stockage

- `NIMBUS_DB_BACKEND`
  - `mongodb` (recommandé) ou `sqlite`.
- `NIMBUS_MONGODB_URI`
  - URI MongoDB.
- `NIMBUS_MONGODB_DB`
  - nom DB.
- `NIMBUS_DB_PATH`
  - utilisé si sqlite.
- `NIMBUS_DATA_DIR`
  - répertoire downloads (dans conteneurs: `/data/downloads`).

### 6.2 Runtime jobs

- `NIMBUS_RUNTIME_ROLE`
  - `api`, `worker`, ou `all`.
- `NIMBUS_MAX_JOBS`
  - concurrence max par worker.
- `NIMBUS_PROVIDER_LIMITS`
  - ex: `copernicus=2,usgs=4`.
- `NIMBUS_QUEUE_POLL_SECONDS`
  - intervalle poller queue.
- `NIMBUS_STALE_JOB_SECONDS`
  - seuil requeue jobs stale.

### 6.3 API

- `NIMBUS_API_KEY`
  - vide = pas d’auth API.
- `NIMBUS_CORS_ORIGINS`
  - origines autorisées.
- `NIMBUS_MAX_REQUEST_MB`
  - limite body request.

### 6.4 Providers download (worker)

- Copernicus:
  - `NIMBUS_COPERNICUS_BASE_URL`
  - `NIMBUS_COPERNICUS_TOKEN_URL`
  - `NIMBUS_COPERNICUS_DOWNLOAD_URL`
  - `NIMBUS_COPERNICUS_USERNAME`
  - `NIMBUS_COPERNICUS_PASSWORD`
- USGS:
  - `NIMBUS_USGS_SERVICE_URL`
  - `NIMBUS_USGS_USERNAME`
  - `NIMBUS_USGS_TOKEN`

### 6.5 UI

- `NIMBUS_SERVICE_URL`
  - URL API vue par UI (compose: `http://nimbus-api:8000`).
- `NIMBUS_UI_DATA_DIR`
  - dossier de résultats vu par UI (`/data/downloads`).
- `NIMBUS_UI_PORT`
  - port hôte UI.

Important: la UI fait aussi du preview provider local, donc elle doit recevoir les credentials providers.

---

## 7. Démarrage propre (Podman)

### 7.1 Pré-requis

```bash
cd "/Users/mehdidinari/Desktop/backend nimbus"
cp .env.example .env
# remplir .env (credentials etc.)
```

### 7.2 Démarrer la machine podman

```bash
podman machine start
```

### 7.3 Relancer stack complète

```bash
./scripts/11_down_stack.sh
./scripts/10_up_stack.sh
```

### 7.4 Vérifier

```bash
podman-compose -f podman-compose.yml ps
curl -s http://127.0.0.1:8000/v1/health | python3 -m json.tool
```

UI:
- [http://127.0.0.1:8501](http://127.0.0.1:8501)

API docs:
- [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

---

## 8. Vérifier que la UI a bien les credentials

Si le preview affiche:
- `missing NIMBUS_COPERNICUS_USERNAME or ... in UI container`

Tester dans le conteneur UI:

```bash
UI_C="$(podman ps --format '{{.Names}}' | grep 'nimbus-ui' | head -n1)"
podman exec "$UI_C" /bin/sh -lc '
for k in NIMBUS_COPERNICUS_USERNAME NIMBUS_COPERNICUS_PASSWORD NIMBUS_USGS_USERNAME NIMBUS_USGS_TOKEN; do
  eval "v=\${$k}"
  if [ -n "$v" ]; then echo "$k=SET"; else echo "$k=MISSING"; fi
done'
```

Si `MISSING`:
- vérifier `.env`,
- `./scripts/11_down_stack.sh` puis `./scripts/10_up_stack.sh`.

---

## 9. Utiliser la UI (workflow recommandé)

### 9.1 Préparation

1. Provider: `Copernicus` ou `USGS`.
2. Mission/collection.
3. Product type.
4. Période courte pour smoke test (2-3 jours).

### 9.2 AOI et tuiles

- Mode AOI:
  - draw sur map,
  - preset square,
  - paste WKT/GeoJSON.
- Optionnel: sélection manuelle de tuiles.
- Si plusieurs tuiles Copernicus sélectionnées:
  - mode batch (`/v1/jobs/batch`, un job par tuile).

### 9.3 Preview

- `Refresh Preview`.
- Si preview vide/erreur, la UI reste opérationnelle pour lancer un job.

### 9.4 Lancer download

- `Start Download`.
- UI affiche cards job:
  - state,
  - progress,
  - bytes,
  - duration,
  - erreurs.

### 9.5 Contrôle

- `Stop`: envoie cancel aux jobs actifs.
- `Reset`: nettoie état runtime UI, garde les fichiers.
- `Unlock`: reset tracker UI.

---

## 10. API jobs — commandes terminal utiles

### 10.1 Health

```bash
curl -s http://127.0.0.1:8000/v1/health | python3 -m json.tool
```

### 10.2 Créer un job

```bash
curl -s -X POST "http://127.0.0.1:8000/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "job_type": "search_download",
    "provider": "copernicus",
    "collection": "SENTINEL-2",
    "product_type": "S2MSI2A",
    "start_date": "2025-01-01",
    "end_date": "2025-01-03",
    "aoi": {"wkt": "POLYGON((-7.8 33.4,-7.8 33.8,-7.2 33.8,-7.2 33.4,-7.8 33.4))"}
  }' | python3 -m json.tool
```

### 10.3 Suivre un job

```bash
JOB_ID="<job_id>"
while true; do
  RESP="$(curl -sS "http://127.0.0.1:8000/v1/jobs/$JOB_ID" || true)"
  if [ -z "$RESP" ]; then
    echo "Empty response (API down or JOB_ID empty)"
  else
    echo "$RESP" | python3 -m json.tool 2>/dev/null || echo "$RESP"
  fi
  echo "-----"
  sleep 2
done
```

Astuce: si `Expecting value: line 1 column 1`, vérifier d’abord:

```bash
echo "JOB_ID=$JOB_ID"
```

### 10.4 Annuler

```bash
curl -s -X DELETE "http://127.0.0.1:8000/v1/jobs/$JOB_ID" | python3 -m json.tool
```

### 10.5 Résultat

```bash
curl -s "http://127.0.0.1:8000/v1/jobs/$JOB_ID/result" | python3 -m json.tool
```

### 10.6 Liste jobs

```bash
curl -s "http://127.0.0.1:8000/v1/jobs?page=1&page_size=50" | python3 -m json.tool
```

Filtrer:

```bash
curl -s "http://127.0.0.1:8000/v1/jobs?state=running&provider=copernicus&page=1&page_size=20" | python3 -m json.tool
```

### 10.7 SSE events

```bash
curl -N "http://127.0.0.1:8000/v1/events"
```

Par job:

```bash
curl -N "http://127.0.0.1:8000/v1/events?job_id=$JOB_ID"
```

### 10.8 Metrics

```bash
curl -s http://127.0.0.1:8000/v1/metrics | head -n 30
```

---

## 11. Vérifier les fichiers téléchargés

```bash
find "/Users/mehdidinari/Desktop/backend nimbus/data/downloads" -type f | head
```

Depuis UI onglet `Results`:
- tableau des jobs,
- listing fichiers,
- download d’un fichier sélectionné.

---

## 12. Logs conteneurs

Podman distant peut limiter `logs` multi-containers en une commande.

Si erreur:
- `logs does not support multiple containers when run remotely`

Faire séparément:

```bash
podman logs -f backendnimbus_nimbus-api_1
```

```bash
podman logs -f backendnimbus_nimbus-worker_1
```

```bash
podman logs -f backendnimbus_nimbus-ui_1
```

Mongo:

```bash
podman logs -f nimbus-mongodb
```

---

## 13. Scale workers (accélération)

```bash
./scripts/12_scale_workers.sh 3
```

Règle pratique:
- parallélisme total approx = `replicas * NIMBUS_MAX_JOBS`
- mais borné par `NIMBUS_PROVIDER_LIMITS` et quotas providers.

---

## 14. Tests

### 14.1 Tests unitaires

Dans ce repo, les tests unitaires sont la base stable:
- `tests/test_models.py`
- `tests/test_engine.py`
- `tests/test_ui_job_runtime.py`
- `tests/test_ui_preview_local.py`

### 14.2 Lancer via Podman

```bash
./scripts/05_test_all.sh
```

---

## 15. Troubleshooting (cas réels)

### A) `watch: command not found` sur macOS

Utiliser:

```bash
while true; do
  curl -s "http://127.0.0.1:8000/v1/jobs/$JOB_ID"
  echo
  sleep 2
done
```

### B) `Expecting value: line 1 column 1`

Cause typique:
- `JOB_ID` vide,
- réponse vide,
- API down.

Check:

```bash
echo "JOB_ID=$JOB_ID"
curl -sS -i "http://127.0.0.1:8000/v1/jobs/$JOB_ID"
```

### C) `proxy already running`

Redémarrer machine podman:

```bash
podman machine stop
podman machine start
```

Puis relancer stack.

### D) `Cannot connect to Podman socket`

```bash
podman system connection list
podman machine start
```

### E) Port UI occupé (`bind: address already in use :8501`)

```bash
lsof -nP -iTCP:8501 -sTCP:LISTEN
PIDS="$(lsof -tiTCP:8501 -sTCP:LISTEN || true)"
[ -n "$PIDS" ] && echo "$PIDS" | xargs kill -9
```

Puis restart stack.

### F) Preview credentials manquants dans UI

Vérifier env dans conteneur UI (section 8), puis down/up stack.

### G) API `/v1/metrics` retourne 404

Soit:
- mauvais path (`/v1/metrics` requis),
- `NIMBUS_ENABLE_METRICS=false`.

---

## 16. Scénario smoke test recommandé (Copernicus)

1. Stack up.
2. Dans UI:
   - provider `Copernicus`, mission `SENTINEL-2`, product `S2MSI2A`,
   - AOI petite zone,
   - période 2-3 jours,
   - `Refresh Preview`,
   - `Start Download`.
3. Dans terminal:
   - récupérer `job_id` via `/v1/jobs` list,
   - suivre `/v1/jobs/{job_id}`,
   - vérifier `/result`.
4. Vérifier fichiers dans `data/downloads`.

Critères OK:
- state passe `queued -> running -> succeeded`,
- progress et bytes montent,
- result contient des `paths`,
- fichiers présents sur disque.

---

## 17. Scénario multi-jobs (batch tuiles Copernicus)

Objectif: accélérer via jobs parallèles.

1. Sélectionner plusieurs tuiles (tile picker).
2. `Start Download`.
3. UI envoie `/v1/jobs/batch`.
4. Vérifier plusieurs `job_id` dans `/v1/jobs`.
5. Suivre progression indépendante par job.

---

## 18. Sécurité et bonnes pratiques

- Ne jamais commit `.env` avec secrets.
- Utiliser `NIMBUS_API_KEY` si API exposée hors local.
- Réduire `NIMBUS_CORS_ORIGINS` à tes origines réelles.
- Limiter `NIMBUS_MAX_JOBS` et `NIMBUS_PROVIDER_LIMITS` selon quotas providers.

---

## 19. Ce qui est prêt vs ce qui est hors scope

### Prêt
- stack Podman 4 services,
- API jobs + SSE,
- UI legacy-like branchée jobs,
- preview local providers,
- tests unitaires.

### Hors scope (dans ce repo fetcher/service/ui)
- pipeline complet harmonization/fusion/interpolation,
- orchestration Airflow/Argo,
- déploiement cloud production complet (OCI/AWS) prêt entreprise.

---

## 20. Commandes de référence (copier-coller)

### Up/down

```bash
cd "/Users/mehdidinari/Desktop/backend nimbus"
podman machine start
./scripts/11_down_stack.sh
./scripts/10_up_stack.sh
```

```bash
./scripts/11_down_stack.sh
```

### Health / docs

```bash
curl -s http://127.0.0.1:8000/v1/health | python3 -m json.tool
open http://127.0.0.1:8000/docs
open http://127.0.0.1:8501
```

### Jobs listing compact

```bash
curl -s "http://127.0.0.1:8000/v1/jobs?page=1&page_size=20" | python3 -c '
import sys,json
b=json.load(sys.stdin)
for i in b.get("items",[]):
    print(i.get("job_id"), i.get("state"), i.get("progress"))'
```

---

Si tu veux, je peux aussi te faire une version 100% "runbook opérationnel" avec uniquement des checklists (pré-prod, prod, incident, rollback) adaptée à ton stage.
