# Polymarket Soccer Goal-Arbitrage Bot

A latency-arbitrage bot for Polymarket soccer markets. The idea: detect a goal
via a low-latency sports data feed before the crowd sees it on TV/social media,
and trade the resulting mispricing before Polymarket's order book adjusts.

See [RESULTS.md](RESULTS.md) for backtest and live-trading numbers.

## How it works

1. **Fast event detection** — a live WebSocket/polling feed (TheSports,
   Sportmonks, or AllSportsAPI) pushes goal events as they happen
2. **Fair-price recompute** — the new score implies a new "fair" win probability;
   the bot compares it to the current Polymarket price
3. **Entry** — if the market hasn't repriced yet, take a position via the CLOB
   API (limit order)
4. **Exit** — close after a fixed time window, or on take-profit/stop-loss

## Architecture

```
.
├── live_system.py          # Main live trading loop (goal → BUY → TP/SL/time exit)
├── main.py                 # CLI entrypoint (live/monitor/backtest/markets)
├── monitor.py               # Standalone live-system monitor
├── run_agents.py            # Multi-agent loop that backtests and tunes
│                            # strategy parameters automatically
├── agents/                  # Agent framework (orchestrator + specialized agents)
│   ├── orchestrator.py       # Coordinates agents, task queue, project state
│   ├── data_agent.py         # Collects & prepares historical match data
│   ├── strategy_agent.py     # Runs backtests, grid-searches parameters
│   ├── code_agent.py         # Applies best params back into config
│   └── reporter_agent.py     # Generates iteration reports
├── src/
│   ├── polymarket/client.py  # Polymarket CLOB client
│   ├── data_provider/        # Pluggable data providers (AllSportsAPI, Sportradar)
│   ├── sportmonks_client.py   # Sportmonks live-goal client
│   ├── thesports_ws.py        # TheSports WebSocket client
│   ├── trading/               # Trading engine + strategy logic
│   ├── backtest/               # Backtest simulator + data loader
│   ├── price_tracker.py        # Captures price curves around goal events
│   └── team_matching.py        # Matches team names across data providers
└── config/settings.py        # Pydantic settings (env-driven)
```

## A note on code state

This is a side project that grew fast during live testing, and it shows in two
places worth being upfront about:

- **`live_system.py` is the script that actually ran in production** (the
  numbers in [RESULTS.md](RESULTS.md) come from it). It's a monolith: it grew
  organically while iterating against live matches, where shipping the next fix
  before the next goal mattered more than architecture.
- **`src/trading/engine.py` is the cleaner, modular rewrite** (pluggable
  strategies and data providers, separated concerns) — it was never promoted to
  live duty before the data-provider trial ended.

There is no automated test suite; validation was done against live markets in
`DRY_RUN` mode. If I took this to production for real, the first steps would be
unifying live trading onto the modular engine and putting tests around the
strategy and client layers.

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# fill in your Polymarket wallet + a data provider API key
```

## Usage

```bash
# Explore live matches / leagues without trading
python main.py live
python main.py leagues
python main.py monitor --duration 60

# Dry run (no real orders)
python main.py run --dry-run

# Live trading (real orders — only after DRY_RUN testing)
python main.py run --live

# Backtest on captured data
python main.py backtest --days 30

# Run the multi-agent parameter-tuning loop
python run_agents.py
```

## Risks

- Real capital at risk; the strategy's edge is latency-sensitive and can flip
  negative if the data feed is too slow (see [RESULTS.md](RESULTS.md))
- Order-book liquidity around goal events is thin and can reprice or vanish in
  under a second
- Always start with `DRY_RUN=true`
