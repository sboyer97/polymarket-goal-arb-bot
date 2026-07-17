# Vérification intégration TheSports

## 1. Architecture API / appels

### Client MQTT (`src/thesports_ws.py`)
- **Protocole** : MQTT over WebSockets (Paho), conforme à la doc TheSports.
- **Connexion** : `mq.thesports.com:443`, TLS, `username_pw_set(user, secret)`, `subscribe(topic)`.
- **Thread** : client MQTT tourne dans un thread dédié (`loop_start()`), callbacks exécutés dans ce thread.
- **Passage vers asyncio** : `asyncio.run_coroutine_threadsafe(coro, loop)` pour appeler les handlers async du live_system depuis le thread MQTT.
- **Payload** : JSON avec `score` / `scores`, `incidents` / `incident` (camelCase / snake_case gérés).

### Ordre de traitement des incidents
1. **Red card** (type contient red/rouge/carton+red) → `on_red_card`
2. **Penalty missed** (penalty + miss/raté) → `on_penalty_missed`
3. **Penalty** (penalty, pas miss) → `on_penalty`
4. **VAR** (type contient var) → `on_var_goal_cancelled` avec `var_result`
5. **Goal** (goal/but) → `on_goal`

Pas de double appel : chaque incident matche une seule branche et `continue`.

### Contexte match pour les incidents
- `score_context` : premier match du tableau `score` du message (home_team, away_team, h, a).
- Si l’incident n’a pas home_team/away_team, on utilise ce contexte.
- `_incident_match_context(inc, score_context)` et `_parse_incident_team(inc)` pour équipe concernée.

---

## 2. Intégration pipeline live (`live_system.py`)

### Démarrage
- **Condition** : `THESPORTS_USER`, `THESPORTS_SECRET`, `THESPORTS_TOPIC` renseignés.
- **Tâche** : `_thesports_ws_loop` est ajoutée **en première** dans la liste des tasks (priorité latence).
- **Config** : host/port lus depuis `THESPORTS_HOST` / `THESPORTS_PORT` (ou dérivés de `THESPORTS_WS_URL`).

### Callbacks TheSports → actions

| Événement   | Callback                        | Action |
|------------|----------------------------------|--------|
| But        | `_on_thesports_goal`            | Dédup (slug, score), `_record_goal_detection_ts("thesports", ...)`, `_on_goal(..., trade_trigger="goal")` → BUY + TP/SL, GoalRecord, backtest 120s, CSV. |
| Carton rouge | `_on_thesports_red_card`     | Dédup `red_{score}`, `_on_goal(..., force_bet_draw=si mène, trade_trigger="red_card")` → même pipeline (BUY nul ou adversaire, TP/SL, GoalRecord, CSV). |
| Penalty    | `_on_thesports_penalty`        | Dédup `penalty_{score}`, `_on_goal(..., force_bet_draw=si adversaire mène, trade_trigger="penalty")` → idem. |
| Penalty raté | `_on_thesports_penalty_missed` | `_request_early_exit(slug, "penalty_missed", "penalty")` → SELL si position ouverte avec trigger=penalty. |
| VAR        | `_on_thesports_var_goal_cancelled` | Sortie toute position sur le slug ; si `_is_var_cancelled(var_result)` → reversion (nul si goal, inverse si penalty/rouge). |

### Enregistrement buts / delta
- **Goal detection** : `_record_goal_detection_ts("thesports", slug, score, home_team, away_team)` uniquement pour les **buts** (`_on_thesports_goal`).
- **CSV** : `goal_detection_times_*.csv` (quand TheSports + Sportmonks ont vu le même score), `goal_delta_thesports_sportmonks_*.csv` (delta en async).
- **Goals CSV** : chaque événement qui passe par `_on_goal` (but, rouge, penalty) crée un `GoalRecord` et est suivi par `_track_prices_after_goal` → `_append_csv(goal)` (fichier goals avec entry/exit/pnl quand dispo).

### Trade
- **Trigger** stocké dans `_live_orders[slug]["trigger"]` (goal / penalty / red_card / var_reversion).
- **Sortie anticipée** : `_request_early_exit(slug, reason, trigger_filter=None|"penalty"|"goal")` + option `reversion_after_exit` pour VAR.
- **Reversion VAR** : `_place_var_reversion_trade(slug, sold_token_id, reason, trigger)` — penalty/red_card → pari inverse (home↔away), goal → nul (ou home/away si on avait nul).

---

## 3. Données envoyées / dispo pour le serveur

- Pas d’envoi HTTP direct vers un serveur depuis le live_system.
- Tout est écrit en local dans **`data/live/`** :
  - `current_matches.json` (état des matchs, mis à jour régulièrement)
  - `live_goals_*.csv` (goals + entry/exit/pnl)
  - `goal_detection_times_*.csv` (détection buts TheSports vs Sportmonks)
  - `goal_delta_thesports_sportmonks_*.csv` (deltas)
  - autres CSV de prix / backtest selon le code existant.
- Sur le serveur de prod, le répertoire `data/live/` est recopié côté local via **rsync** (voir `server_logs/README.md`). Donc toute donnée TheSports (buts, rouge, penalty, VAR, reversion) est reflétée dans ces fichiers et remonte via ce rsync.

---

## 4. Points vérifiés / cohérence

- Dédup : buts `(slug, new_score)` ; rouge `(home, away, "red_1-0")` ; penalty `(home, away, "penalty_1-0")` dans `_goal_seen`.
- Un seul BUY par événement grâce à `_goals_traded` et `trade_key_override` pour red/penalty.
- Écart max 1 : `skip_bet_ecart` dans `_on_goal` pour tous les types (goal, red, penalty).
- VAR : sortie toute position (goal/penalty/red_card), reversion selon trigger (nul vs inverse).
- Commentaire `_goal_seen` mis à jour pour refléter les deux formats de clés.

---

## 5. Dépendances

- **paho-mqtt** dans `requirements.txt`.
- Variables d’environnement : `THESPORTS_HOST`, `THESPORTS_PORT`, `THESPORTS_USER`, `THESPORTS_SECRET`, `THESPORTS_TOPIC` (ou équivalents selon le code).
