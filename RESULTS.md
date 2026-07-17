# Results

## Signal validation (backtest on live-captured odds)

Between March 7-9 2026, the goal-detection pipeline was run live (WebSocket feed +
Polymarket order book polling) without placing real orders, recording the actual
market price at goal time and at fixed intervals after (0s / 1s / 5s / 10s / 30s /
60s / 120s). This isolates whether the *signal* has edge, independent of execution.

- 129 goals detected, 84 with a tradeable market at goal time
- **Win rate: 64.3%** (54 winners / 30 losers)
- Simulated PnL at $10/trade using observed entry/exit prices: **+$1,004**
- Best average exit window: ~60s after the goal

## Live execution (real capital, real orders)

Between Feb 17 - Mar 12 2026 the bot traded live on Polymarket with real capital
(CLOB API, limit orders). Results on soccer markets specifically:

- 115 buys / 86 sells / 7 redemptions, $497 gross volume
- **Net PnL: -$75 (-15%)**

## The gap, and why it matters

The backtest assumes a fill at the exact price observed at T+0. In live trading,
the 2-3s latency of the data feed (mid-tier providers; sub-second feeds run
$6k+/month, outside this project's budget) meant the order book had often already
partially or fully repriced by the time an order reached it — the same order-book
"flash wipe" effect after a goal that any market maker on this asset class has to
account for.

In short: the signal itself is real and repeatable (64% win rate on live-observed
prices), but capturing it in practice is bottlenecked by data latency and
execution speed, not by strategy design. That gap — and closing it — is the
actual engineering problem.
