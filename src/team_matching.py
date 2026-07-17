"""
Unified team-name matching for Polymarket soccer bot.

Used by live_system.py (TheSports/Sportmonks → Polymarket slug),
price_tracker.py (Gamma event → token resolution), and scripts.

Uses rapidfuzz for fuzzy matching after cleaning names (accents, noise words).
"""

import unicodedata
from rapidfuzz import fuzz

# ── Noise words stripped before comparison ──────────────────────────────────
# Club suffixes, articles, prepositions — they add no discriminating signal.
NOISE_WORDS: frozenset[str] = frozenset({
    # Club suffixes / prefixes
    "fc", "afc", "sc", "sk", "cf", "ac", "cfc", "sv", "bc", "fk",
    "as", "ss", "nk", "bk", "ud", "cd", "rc", "us", "ca", "cs",
    "aa", "sd", "se", "ssc", "rcd", "vfl", "vfb", "bv", "tsv",
    "1.", "sco", "osc",
    # Articles / prepositions
    "de", "la", "el", "du", "le", "des", "di", "del", "van", "von",
    "da", "do", "dos", "das",
})

# ── Abbreviation expansions ─────────────────────────────────────────────────
# Only for true abbreviations where fuzzy matching cannot bridge the gap.
# Keys must be the *cleaned* form (lowercase, accents stripped, noise removed).
ABBREVIATIONS: dict[str, str] = {
    "qpr": "queens park rangers",
    "spurs": "tottenham hotspur",
    "wolves": "wolverhampton wanderers",
    "man utd": "manchester united",
    "man city": "manchester city",
    "man united": "manchester united",
    "west ham": "west ham united",
    "forest": "nottingham forest",
    "boro": "middlesbrough",
    "brighton": "brighton hove albion",
    "palace": "crystal palace",
    "villa": "aston villa",
    "psv": "psv eindhoven",
    "ajax": "ajax amsterdam",
    "barca": "barcelona",
    "atletico": "atletico madrid",
    "real": "real madrid",
    "inter": "internazionale milano",
    "inter milan": "internazionale milano",
    "lazio": "lazio roma",
    "roma": "roma",
    "juve": "juventus",
    "napoli": "napoli",
    "psg": "paris saint-germain",
    "om": "olympique marseille",
    "ol": "olympique lyonnais",
    "lyon": "olympique lyonnais",
    "marseille": "olympique marseille",
    "dortmund": "borussia dortmund",
    "gladbach": "borussia monchengladbach",
    "leverkusen": "bayer leverkusen",
    "bayern": "bayern munchen",
    "leipzig": "rasenballsport leipzig",
}


def clean_team_name(name: str) -> str:
    """Normalize: NFKD→ASCII, lowercase, strip noise words and short year numbers."""
    if not name:
        return ""
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    s = s.lower().strip()
    s = s.replace("-", " ").replace("'", "").replace(".", " ")
    words = [
        w for w in s.split()
        if w not in NOISE_WORDS
        and not (w.isdigit() and len(w) <= 4)
    ]
    return " ".join(words) if words else s


def _expand(cleaned: str) -> str:
    """Expand known abbreviations on the cleaned name."""
    return ABBREVIATIONS.get(cleaned, cleaned)


# Age-group / gender tags that distinguish different squads of the same club
_SQUAD_TAGS: frozenset[str] = frozenset({
    "u17", "u18", "u19", "u20", "u21", "u23",
    "women", "w", "feminin", "feminine", "femenino", "femenina",
    "reserves", "reserve", "ii", "b",
})


def _has_squad_tag(cleaned: str) -> str | None:
    """Return the squad tag if present, else None."""
    for w in cleaned.split():
        if w in _SQUAD_TAGS:
            return w
    return None


def teams_match(a: str, b: str, threshold: int = 82) -> bool:
    """
    True if team names *a* and *b* likely refer to the same club.

    Strategy (ordered, first hit wins):
      1. Reject if squad tags mismatch (U19 vs senior, Women vs Men).
      2. Exact match on cleaned + expanded names.
      3. Substring containment (both >= 4 chars).
      4. rapidfuzz token_set_ratio >= threshold.
    """
    if not a or not b:
        return False
    ca, cb = clean_team_name(a), clean_team_name(b)
    if not ca or not cb:
        return False

    # 0) Squad-tag mismatch → reject (Real Betis ≠ Real Betis U19)
    tag_a, tag_b = _has_squad_tag(ca), _has_squad_tag(cb)
    if tag_a != tag_b:
        return False

    ea, eb = _expand(ca), _expand(cb)

    # 1) Exact
    if ea == eb:
        return True

    # 2) Substring containment (e.g. "atalanta" in "atalanta bergamasca calcio")
    if len(ea) >= 4 and len(eb) >= 4 and (ea in eb or eb in ea):
        return True

    # 3) Fuzzy: token_set_ratio handles word reordering + partial overlap
    score = fuzz.token_set_ratio(ea, eb)
    return score >= threshold
