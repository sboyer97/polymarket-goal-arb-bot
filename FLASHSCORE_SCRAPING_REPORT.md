# FlashscoreScraping – Non-interactive use report

## 1. Repo location

- **Cloned to:** `/tmp/FlashscoreScraping`
- **Alternative:** You can use a sibling folder, e.g. `"$(dirname "$(pwd)")/FlashscoreScraping"` from the Polymarket sport project.

---

## 2. How to run NON-INTERACTIVELY

The scraper **already supports non-interactive use** when both `country` and `league` are provided. It reads **CLI arguments** (not env) in `key=value` form.

- **Entry:** `node src/index.js` (npm script: `npm run start` → same)
- **Parsing:** `src/cli/arguments/index.js` reads `process.argv.slice(2)` and parses `country=`, `league=`, `fileType=`, etc.
- **Prompts:** `inquirer` is used only when a value is missing. If you pass `country` and `league`, `fileType` is optional (defaults to JSON). No prompts are shown.

**Required for no prompts:** pass both `country=` and `league=`.

---

## 3. Exact command and cwd

**Working directory:** must be the FlashscoreScraping repo root (so that `./src/data` and Playwright work as intended).

```bash
cd /tmp/FlashscoreScraping
node src/index.js country=england league=premier-league fileType=json
```

Or with npm:

```bash
cd /tmp/FlashscoreScraping
npm run start -- country=england league=premier-league fileType=json
```

**Optional args:** `concurrency=10`, `saveInterval=10`, `headless=true` (default), `fileType=json` (default).  
`fileType` can be `json`, `json-array`, or `csv`.

---

## 4. Where output goes (file, not stdout)

- **Output is only to a file.** Nothing is written to stdout except log messages and a final path line (see below).
- **Path:** `<cwd>/src/data/<fileName><extension>`
  - `OUTPUT_PATH` default: `./src/data` (see constants).
  - `fileName` = country + league, normalized (e.g. `england_premier_league`).
  - Extension: `.json` for `fileType=json`, `.array.json` for `json-array`, `.csv` for `csv`.
- **Example:**  
  Cwd = `/tmp/FlashscoreScraping`  
  → file: `/tmp/FlashscoreScraping/src/data/england_premier_league.json`
- **During run:** Data is saved every `saveInterval` matches and again at the end (see `writeDataToFile` in `src/index.js`).

---

## 5. Patches applied in FlashscoreScraping (for Python integration)

Two small changes were made in the cloned repo only (no changes in the Polymarket sport project):

1. **`src/constants/index.js`**  
   - `OUTPUT_PATH` can be overridden by env:  
     `OUTPUT_PATH = process.env.FLASHSCORE_OUTPUT_PATH || "./src/data"`  
   - So from Python you can set `FLASHSCORE_OUTPUT_PATH` to a known directory and then read the file from there.

2. **`src/index.js`**  
   - After “Data collection and file writing completed!”, the script prints a single parseable line:  
     `FLASHSCORE_OUTPUT_FILE=<absolute path to the output file>`  
   - Your Python script can run the Node process, capture stdout, and parse this line to get the exact path to the JSON (or CSV) file.

---

## 6. Using from Python (summary)

1. **Set cwd** to the FlashscoreScraping repo (e.g. `/tmp/FlashscoreScraping`).
2. **Run:**  
   `node src/index.js country=england league=premier-league fileType=json`  
   Optionally set `FLASHSCORE_OUTPUT_PATH` to control where the file is written.
3. **Get the output file:**  
   - Either parse the last line of stdout for `FLASHSCORE_OUTPUT_FILE=<path>`, or  
   - If you set `FLASHSCORE_OUTPUT_PATH`, build the path as  
     `os.path.join(os.environ["FLASHSCORE_OUTPUT_PATH"], "england_premier_league.json")`  
     (fileName is predictable from country + league: lowercased, non-alphanumeric replaced by `_`).
4. **Read JSON:** open that path and parse with `json.load()`.

No further patches are required for non-interactive use; the optional env and the `FLASHSCORE_OUTPUT_FILE` line are for convenience from Python.

---

## 7. Note on “live” scores

The scraper loads the league’s **results** and **fixtures** pages and scrapes match data (including `status` and `result`). It does not stream live updates. To approximate “live” scores from Python, run this command periodically (e.g. every 1–5 minutes) and compare the latest JSON with the previous one to detect new or updated results.
