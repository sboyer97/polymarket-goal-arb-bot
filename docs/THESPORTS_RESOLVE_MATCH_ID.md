# TheSports : résoudre match_id → (home_team, away_team)

Le flux MQTT TheSports envoie des matchs avec un **id** (ex. `vjxm8ghekwewr6o`) et des champs **score**, **stats**, **incidents**, **tlive** — mais **pas les noms d’équipes** dans ce payload. Pour appeler les callbacks (but, carton rouge, penalty, VAR), il faut associer chaque `match_id` à un couple `(home_team, away_team)`.

## Comment ça marche dans le code

1. **Cache** : `live_system` garde un dictionnaire `_thesports_id_to_teams: match_id → (home_team, away_team)`.
2. **Resolver** : la fonction passée à `run_thesports_ws_loop(..., resolve_match_id=...)` fait un simple lookup dans ce cache (synchrone, appelée depuis le thread MQTT).
3. **Remplissage du cache** : si tu configures une **URL API REST** TheSports qui renvoie la liste des matchs (ou le détail d’un match) avec **id + noms d’équipes**, le live appelle cette URL périodiquement et remplit le cache.

## Créer un `resolve_match_id` via l’API TheSports

### 1. Obtenir l’endpoint côté TheSports

Il faut une **requête REST** TheSports qui, pour les matchs en direct (ou pour un match donné), renvoie au moins :

- un **id** (le même que dans le flux MQTT),
- le **nom de l’équipe domicile**,
- le **nom de l’équipe extérieur**.

Souvent c’est soit :

- un **liste de matchs en direct** (ex. “live matches”, “matches in-play”),  
- soit un **détail d’un match par id** (ex. “get match by id”).

Dans les deux cas, la réponse doit contenir pour chaque match : `id` (ou `match_id`) et deux champs type `home_team` / `away_team` ou `strHomeTeam` / `strAwayTeam` (ou équivalents).

À vérifier dans la **doc TheSports** (ou en demandant au support) :

- l’**URL exacte** (base + path),
- les **paramètres** (souvent `user` et `secret` en query, comme dans ta spec),
- le **format de la réponse** (ex. `{ "code": 200, "results": [ { "id", "home_team", "away_team" }, ... ] }`).

### 2. Utiliser « Schedule and Results - date query » (recommandé)

La doc TheSports propose l’endpoint **Schedule and Results - date query** (package BASIC DATA) qui renvoie les matchs pour une date avec **id** et noms d’équipes :

- **Doc** : [TheSports Football API – Schedule and Results (date query)](https://www.thesports.com/fr/docs/football#package:BASIC%20DATA,endpoint:Schedule%20and%20Results%20-%20date%20query)

Configure dans ton **`.env`** l’URL de l’endpoint (sans le paramètre `date`) :

```env
THESPORTS_API_SCHEDULE_URL=https://api.thesports.com/.../schedule
```

Le code appelle cette URL avec `user`, `secret` (depuis `THESPORTS_USER` / `THESPORTS_SECRET`) et **`date=YYYY-MM-DD`** (aujourd’hui et demain en UTC). Le cache est rempli au démarrage puis rafraîchi toutes les 2 minutes.

### 3. Alternative : URL liste de matchs

- **Variable** : `THESPORTS_API_MATCH_LIST_URL=https://api.thesports.com/.../live`
- Le code ajoute **user** et **secret** en query. Tu peux utiliser **soit** `THESPORTS_API_SCHEDULE_URL` **soit** `THESPORTS_API_MATCH_LIST_URL` (ou les deux).

### 4. Format de réponse attendu

Le code accepte des réponses de ce type (ou proches) :

- **Liste de matchs** dans l’un des champs : `results`, `data` ou `matches` (tableau d’objets).
- Pour chaque objet de ce tableau :
  - **id** : `id` ou `match_id` (string, même valeur que dans le flux MQTT).
  - **Équipe domicile** : `home_team` ou `strHomeTeam` ou `homeTeam`.
  - **Équipe extérieur** : `away_team` ou `strAwayTeam` ou `awayTeam`.

Exemple minimal :

```json
{
  "code": 200,
  "results": [
    {
      "id": "vjxm8ghekwewr6o",
      "home_team": "Liverpool",
      "away_team": "Chelsea"
    }
  ]
}
```

Si ta doc utilise d’autres noms de champs, il suffit qu’ils soient cohérents pour tous les matchs ; on peut étendre le code pour accepter d’autres noms si tu les indiques.

### 5. Résumé : “créer” le resolve_match_id

- **Côté code** : le `resolve_match_id` est déjà en place : c’est la lecture du cache `_thesports_id_to_teams` (méthode `_resolve_thesports_match_id`).
- **Côté toi** :
  1. Trouver dans la **doc TheSports** l’endpoint REST qui renvoie les matchs (live ou par id) avec **id + home_team + away_team** (ou noms équivalents).
  2. Définir **`THESPORTS_API_MATCH_LIST_URL`** avec cette URL.
  3. Redémarrer le live ; le cache se remplit et le resolver renverra bien `(home_team, away_team)` pour chaque `match_id` reçu en MQTT.

Si l’API TheSports ne propose qu’un “get match by id”, on peut adapter le code pour appeler cet endpoint par id (et mettre en cache le résultat) au lieu d’une seule liste — il suffit de connaître l’URL exacte et le format de réponse.
