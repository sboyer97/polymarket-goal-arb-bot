# Implémentation – Live System Polymarket Sport

Documentation de ce qui a été implémenté pour le backtest live sur les buts (Polymarket + Sportmonks) : collecte de données, suivi des prix, détection des buts, analyse et monitoring.

---

## 1. Vue d’ensemble

- **Objectif** : mesurer en conditions réelles combien de temps les cotes Polymarket mettent à se stabiliser après un but, et si une fenêtre de trading (entrée T+0, sortie T+60s) serait rentable. **Aucun trading réel** : enregistrement des prix et des buts uniquement.
- **Composants principaux** :
  - **live_system.py** : orchestration (WebSocket Polymarket, API Gamma, Sportmonks, boucle de prix, écriture CSV/JSON).
  - **src/price_tracker.py** : résolution des marchés (Gamma/CLOB), récupération ask/bid, analyse des courbes de prix après but.
  - **src/sportmonks_client.py** : API Sportmonks (matchs en direct, scores, événements but).
  - **monitor.py** : tableau de bord (matchs suivis, buts récents, analyse prix, statut du système).

---

## 2. Sources de données

### 2.1 WebSocket Polymarket (`wss://sports-api.polymarket.com/ws`)

- Connexion au flux sport Polymarket.
- Réception des mises à jour d’état des matchs : **score**, **period** (1H, 2H, HT), **elapsed** (minute), **live**, **ended**.
- Un message est accepté comme « match en cours » si : `live` OU `status inprogress` OU period ∈ {1H, 2H, HT} OU score ≠ 0-0.
- Les matchs ainsi reçus sont ajoutés/mis à jour dans `_matches` et servent à :
  - alimenter le **monitor** (period/elapsed = « matchs suivis »),
  - alimenter le **heartbeat** (compteur « matchs soccer suivi(s) »),
  - être associés aux buts Sportmonks via `_find_slug_by_teams` (home/away).

### 2.2 API Gamma (`https://gamma-api.polymarket.com`)

- **Rôle** : compléter la liste des matchs en direct sans dépendre uniquement du WebSocket (événements déjà ouverts mais pas encore dans le flux).
- **Endpoints utilisés** :
  - `GET /sports` : liste des sports/ligues et leurs `tags`.
  - `GET /events?tag_id=X&closed=false&limit=100` : événements « non fermés » par ligue.
- **Fonction** : `get_live_soccer_events_from_gamma(league_slugs)` dans `price_tracker.py` retourne une liste `{slug, home_team, away_team, league, score}`.
- **Rafraîchissement** : `_gamma_refresh_loop` appelle `_fetch_live_matches_from_gamma()` au démarrage puis toutes les **2 minutes** ; les nouveaux matchs sont ajoutés à `_matches`.

### 2.3 Sportmonks (`https://api.sportmonks.com/v3/football`)

- **Rôle** : détection des **buts** en temps réel (Polymarket ne fournit pas les événements but, seulement le score).
- **Client** : `SportmonksClient` dans `src/sportmonks_client.py` ; méthode `get_inplay_livescores()` pour les matchs en cours.
- **Détection** :
  - Changement de score (home/away) entre deux polls → but attribué à l’équipe dont le score a augmenté.
  - Événements de type GOAL / OWN_GOAL / PENALTY / etc. pour éviter les doublons et affiner (minute, etc.).
- **Correspondance** : les noms d’équipes Sportmonks sont rapprochés de ceux de `_matches` via `_find_slug_by_teams` (normalisation, sous-chaînes, premier mot) pour obtenir le **slug** Polymarket et enregistrer le but côté live_system.

---

## 3. Filtres et qualité des matchs

### 3.1 Exclure les « more-markets » (doublons)

- Les slugs du type `…-more-markets` (ex. `bun-wer-mai-2026-03-15-more-markets`) sont des doublons d’un même match.
- **Filtrage** :
  - Dans `get_live_soccer_events_from_gamma()` : on ne retourne pas les events dont le slug contient `-more-markets`.
  - Dans `_fetch_live_matches_from_gamma()` : on n’ajoute pas un match dont le slug contient `-more-markets`, et on **supprime** de `_matches` les slugs déjà présents contenant `-more-markets` (nettoyage périodique).

### 3.2 Ne garder que les matchs déjà commencés

- L’API Gamma avec `closed=false` peut renvoyer des matchs à venir.
- **Filtrage** dans `get_live_soccer_events_from_gamma()` :
  - Si pas de `startDate` / `start_date` → event ignoré (on ne peut pas vérifier).
  - Si `startDate` dans le **futur** (UTC) → event ignoré.
  - Sinon le match est considéré comme commencé et peut être ajouté à `_matches`.

### 3.3 « Matchs suivis » (ceux pour lesquels on a des données)

- **Définition** : un match est « suivi » si on a reçu au moins une mise à jour WebSocket avec **period** ou **elapsed** (données live Polymarket).
- **Utilisation** :
  - **Monitor** : n’affiche que les matchs avec `period` ou `elapsed` dans `current_matches.json` (titre « Matchs suivis (N) »).
  - **Heartbeat** : le message « En écoute… X match(s) soccer suivi(s) » compte uniquement les matchs ayant `period` ou `elapsed`, et non plus `len(_matches)`.

---

## 4. Collecte des prix

### 4.1 Boucle de prix (toutes les secondes)

- **`_live_price_poll_loop`** : pour chaque match dans `_matches`, résolution des tokens CLOB (Home/Draw/Away) via `PriceTracker.find_tokens_for_match`, puis récupération **ask** et **bid** pour chaque outcome (cache par slug dans `_token_cache`).
- Une ligne est écrite par seconde dans le CSV **realtime** pour chaque match (même si certains prix sont vides).

### 4.2 CSV Realtime

- **Fichier** : `data/live/live_prices_realtime_YYYYMMDD.csv`.
- **Colonnes** : `timestamp_utc`, `slug`, `home_team`, `away_team`, `home_ask`, `home_bid`, `draw_ask`, `draw_bid`, `away_ask`, `away_bid`.
- **Comportement** : création du fichier avec header au premier écrit ; pas d’écrasement au redémarrage (append par jour).

### 4.3 Buffer « 3 minutes avant le but »

- Pour chaque match, un buffer circulaire (`deque`, max 181 éléments) stocke les **dernières 3 minutes** de prix (timestamp + 6 prix ask/bid).
- À la détection d’un but (Sportmonks), on enregistre les données **avant** but (de -180s à 0s) depuis ce buffer, et on lance le suivi **après** but (voir section 6).

### 4.4 CSV 3 minutes (autour du but)

- **Fichier** : `data/live/live_prices_3min_YYYYMMDD.csv`.
- **Contenu** : une ligne par (but, seconde relative au but) avec les prix Home/Draw/Away (ask/bid).
- **Colonnes** (ex.) : `goal_timestamp`, `match_slug`, `home_team`, `away_team`, `scoring_team`, `match_period`, `second_relative_to_goal`, `at_goal`, `price_home_ask`, `price_home_bid`, … , `sample_timestamp_utc`.
- Les données « après but » sont collectées pendant 3 minutes (pending `_pending_goal_prices`), puis écrites en bloc via `_write_prices_around_goal`.

---

## 5. Détection des buts et enregistrement

- **Source** : Sportmonks (poll régulier), pas le WebSocket Polymarket.
- **Quand un but est détecté** :
  1. On récupère le slug Polymarket via `_find_slug_by_teams`.
  2. On crée un `GoalRecord` (timestamp, match, league, home/away, scoring_team, minute, score_before/after, entry_odds/exit_odds/pnl à `None` au départ).
  3. On enregistre la ligne dans le CSV des buts (`live_goals_*.csv`).
  4. On lance **`_track_prices_after_goal`** : enregistrement du buffer « avant » + création d’un pending pour collecter les prix « après » (jusqu’à 3 min), puis analyse par le PriceTracker (voir section 6).
  5. Quand le PriceTracker a fini (120s), on met à jour le `GoalRecord` avec les **vrais** entry (ask T+0), exit (bid T+60), PnL réel (fees/slippage déduits), et on écrit la ligne backtest / rapport dans WORK_LOG.

---

## 6. Price Tracker – Analyse post-but

- **Objectif** : répondre à « combien de temps pour que le prix se stabilise ? » et « peut-on trader (entrée T+0, sortie T+60) ? ».
- **Échantillonnage** : T+0, 1, 2, 3, 5, 7, 10, 15, 20, 30, 45, 60, 90, 120 secondes (ask/bid pour Home, Draw, Away).
- **Métriques** :
  - `time_to_stabilize_seconds` (seuil ~0,5 % sur 10s),
  - `max_price_seconds` / `min_price_seconds`,
  - `entry_ask_0s`, `exit_bid_60s`,
  - `profit_if_entry_0s_exit_60s` (%), etc.
- **PnL simulé** : `(exit_bid / entry_ask - 1) * bet - fees - slippage` ; utilisé pour le CSV buts et le WORK_LOG, pas pour du trading réel.
- **Fichiers** : `price_curves_YYYYMMDD.csv` (une ligne par but avec métriques), et rapport texte dans WORK_LOG.

---

## 7. Fichiers de données et sorties

| Fichier | Description |
|--------|--------------|
| `data/live/current_matches.json` | Liste des matchs (slug, home, away, score, league, period, elapsed) ; écrit par le live_system, lu par le monitor. |
| `data/live/live_goals_*.csv` | Un fichier par session : chaque but avec timestamp, match, minute, score, entry/exit, PnL, cumulative PnL. |
| `data/live/live_prices_realtime_YYYYMMDD.csv` | Prix (ask/bid) par seconde pour tous les matchs du jour. |
| `data/live/live_prices_3min_YYYYMMDD.csv` | Prix autour de chaque but (-180s à +180s), par seconde relative. |
| `data/live/price_curves_YYYYMMDD.csv` | Une ligne par but : métriques de stabilisation et profit. |
| `WORK_LOG.md` | Rapports d’analyse live (dernier but, métriques, verdict « assez de temps pour trader » ou non). |

---

## 8. Monitor et heartbeat

- **Monitor** (`python monitor.py`) :
  - Statut du processus `live_system.py` (🟢 / 🔴).
  - **Matchs suivis** : uniquement ceux avec `period` ou `elapsed` dans `current_matches.json`.
  - Dernier CSV de buts, nombre de buts, derniers buts (time, match, min, score, PnL, total).
  - Dernière analyse prix (stabilisation, meilleur exit, profit T+0→60, verdict).
  - Rafraîchissement toutes les 10 secondes.
- **Heartbeat** (dans live_system, toutes les 30s) : affiche « En écoute… N match(s) soccer suivi(s) » avec N = nombre de matchs ayant `period` ou `elapsed`.

---

## 9. Nettoyage et durée de vie

- **Matchs terminés** : quand le WebSocket envoie `ended`, on pose `ended_at` sur le match ; après **5 minutes**, le match est retiré de `_matches` et des caches (price_buffers, token_cache).
- **Events Gamma fermés** : `_fetch_live_matches_from_gamma` récupère aussi les events `closed=true` et retire ces slugs de `_matches` (plus nettoyage des « more-markets »).

---

## 10. Scripts d’analyse

- **`scripts/plot_price_after_goal.py`** : lit `live_prices_realtime_*` (180s avant) et `live_prices_3min_*` (après but), trace la courbe du prix (équipe qui a marqué) autour du but ; options `--max-goals`, `--out`.
- **`scripts/compare_goal_latency_sportmonks_flashscore.py`** : compare la latence de détection des buts entre **Sportmonks** (API) et **Flashscore** (via [FlashscoreScraping](https://github.com/gustavofariaa/FlashscoreScraping)). Monitor des matchs en direct : enregistre l’heure à laquelle chaque source voit le nouveau score, puis affiche qui a été le plus rapide et de combien (secondes). Options : `--flashscore-json` (fichier JSON mis à jour par un processus externe), ou `--flashscore-repo` + `--country` + `--league` pour lancer le scraper Node nous-mêmes.
- Autres scripts : tests Gamma (`test_gamma_by_id.py`, `test_gamma_market.py`), Sportmonks inplay (`test_sportmonks_inplay.py`), mapping live (`check_live_mapping.py`), etc.

---

## 11. Trading live (CLOB Polymarket)

### 11.1 Authentification

- **Option A – EOA + clés API** : `POLYMARKET_API_KEY`, `POLYMARKET_API_SECRET`, `POLYMARKET_API_PASSPHRASE`, `POLYMARKET_PRIVATE_KEY` (clé du wallet ayant créé cette API key).
- **Option B – Magic + Smart Wallet** : `POLYMARKET_PRIVATE_KEY` (EOA signer) + `POLYMARKET_SMART_WALLET` (adresse du profil Polymarket). Les credentials API sont **dérivés** au démarrage via `create_or_derive_api_creds()` (pas besoin de les mettre dans .env). Utilise `signature_type=1` (POLY_PROXY) et `funder=smart_wallet`.

### 11.2 Ordres et sortie

- À chaque but détecté (et si CLOB initialisé et `DRY_RUN=false`) : **BUY** sur l’outcome de l’équipe qui marque (montant = `min(bet_amount, MAX_POSITION_SIZE)`), puis boucle de sortie **TP/SL/time**.
- **Take Profit** : `TAKE_PROFIT_PCT` (défaut +3 %) — sortie quand le rendement non réalisé ≥ ce seuil.
- **Stop Loss** : `STOP_LOSS_PCT` (défaut -15 %) — sortie quand le rendement non réalisé ≤ ce seuil.
- **Time exit** : après `exit_after` secondes (défaut 60 s), sortie forcée.
- La boucle interroge le **bid** (prix de vente) toutes les 5 s et place un **SELL** dès qu’une des trois conditions est atteinte.

### 11.3 Fichier de référence

- Le dossier **bot polymarket** contient un exécuteur CLOB qui tourne bien : `src/execution/polymarket_executor.py` (Magic + Smart Wallet, `place_limit_order`, `place_market_order`), et `live_engine.py` / `live_executor.py` pour la logique TP/SL (take_profit, stop_loss, time_exit).

---

## 12. Résumé des points clés

1. **Backtest / enregistrement** : CSV, WORK_LOG, pas d’ordres si DRY_RUN ou CLOB non initialisé.
2. **Trading live** : BUY au but, sortie sur TP (+%), SL (-%) ou time (exit_after s) ; auth EOA ou Magic+Smart Wallet.
3. **Matchs** : WebSocket + Gamma (filtrés : pas more-markets, pas à venir) ; « suivis » = avec period/elapsed.
4. **Buts** : détectés par Sportmonks (et AllSportsAPI, Polymarket WS), associés au slug par noms d’équipes.
5. **Prix** : realtime (1/s) + buffer 3 min avant but + 3 min après but en CSV 3min ; ask/bid pour Home/Draw/Away.
6. **PnL** : calculé a posteriori (entry_ask T+0, exit_bid T+60) pour évaluer la fenêtre ; en live, PnL réel via TP/SL/time.
