# Athena's Pallas (`athenas-pallas`)

Open-source Rust framework for **event-driven** algorithmic trading: **live**, **paper**, and **backtest** share the same strategy and risk hooks; you swap **connectors** and the [`ExecutionGateway`](athenas-pallas/src/execution/mod.rs).

## Quickstart

```bash
cargo test -p athenas-pallas
```

REST signing + Wiremock integration tests (requires composite feature):

```bash
cargo test -p athenas-pallas --features binance-live
```

### Paper trading (live public Binance data + local execution)

```bash
cargo run -p paper_binance
```

Optional control plane (same process as the example, requires `paper_binance` built with `control-server`):

- `POST http://127.0.0.1:9847/pause` — pause new orders (header `x-pallas-secret`, value from `PALLAS_CONTROL_TOKEN` or default `dev`).
- `POST .../resume` — resume.
- `POST .../cancel-all` — cancel all open (paper) orders.
- `POST .../flatten` — cancel all, then submit reduce-only market orders to flatten positions (flatten intents bypass pause).

### Live Binance Spot (REST + user stream)

**Warning: this path can spend real funds.** Prefer Binance Spot **testnet** URLs until you deliberately point at mainnet.

```bash
cargo run -p live_binance
```

Environment variables (never commit keys):

| Variable | Purpose |
|----------|---------|
| `BINANCE_BASE_URL` | REST API root (default in example: `https://testnet.binance.vision`) |
| `BINANCE_WS_URL` | Stream WebSocket root (default: `wss://testnet.binance.vision`) |
| `BINANCE_API_KEY` | API key (`X-MBX-APIKEY`) |
| `BINANCE_SECRET` | HMAC signing secret |
| `PALLAS_CONTROL_TOKEN` | Optional localhost control secret (default `dev`) |

Public streams use combined trade + book ticker; depth snapshots use `@depth{N}@100ms` (`N` ∈ {5,10,20}) via [`BinanceDepthStream`](athenas-pallas/src/connectors/binance_spot.rs). User data uses `POST /api/v3/userDataStream` + keepalive `PUT` (~every 30 minutes in the example task).

### Backtest + metrics

```bash
cargo run -p backtest_csv
```

Prints PnL, max drawdown, Sharpe, Sortino, and per-step returns from a toy OHLC path.

Parallel batch replay API: [`backtest::batch`](athenas-pallas/src/backtest/batch.rs) (`run_scenarios_parallel`, `RunReport`).

## Risk: max daily loss (equity, UTC)

[`MaxDailyLossQuote`](athenas-pallas/src/risk.rs) compares **mark-to-market equity** in a quote asset against the **UTC calendar day** anchor stored in [`GlobalState::risk_day_anchor`](athenas-pallas/src/state.rs). Set `GlobalState::daily_risk_quote` to the same quote asset (e.g. USDT). The engine refreshes the anchor on market/account/timer ticks (UTC date rollover resets the day-start equity). Loss is **day-start equity minus current equity**; orders are rejected if loss exceeds `max_loss`. Intents with [`OrderIntentSource::Flatten`](athenas-pallas/src/events.rs) bypass this check (emergency flatten).

## Architecture

- **Engine** — single consumer loop: market data → optional passive fills → `Strategy` → `RiskPipeline` → `ExecutionGateway`.
- **State** — [`GlobalState`](athenas-pallas/src/state.rs): L1/L2/trade cache, balances, positions, open orders, optional daily risk anchor.
- **Modes** — [`PaperGateway`](athenas-pallas/src/execution/paper.rs), [`SimGateway`](athenas-pallas/src/execution/sim.rs), [`LiveGateway`](athenas-pallas/src/execution/live.rs) (stub when only `live-trading`), or [`BinanceLiveGateway`](athenas-pallas/src/execution/binance_live.rs) (with feature `binance-live`).
- **Timers** — [`EngineConfig::timer_schedules`](athenas-pallas/src/engine.rs) spawns `tokio::time::interval` tasks that emit [`TimerEvent { ts, id }`](athenas-pallas/src/events.rs).

## Features

| Feature | Purpose |
|---------|---------|
| `binance` | Binance Spot public WebSocket connectors (`BinanceCombinedStream`, `BinanceDepthStream`) |
| `control-server` | Localhost Axum control API |
| `live-trading` | `reqwest` client; generic [`LiveGateway`](athenas-pallas/src/execution/live.rs) stub if `binance-live` is off |
| `binance-live` | `binance` + `live-trading` + `hmac` / `sha2` / `hex`; [`BinanceLiveGateway`](athenas-pallas/src/execution/binance_live.rs), [`BinanceUserDataStream`](athenas-pallas/src/connectors/binance_user_data.rs) |

## Security

- **Never commit API keys.** Use environment variables and OS secret stores.
- Control endpoints are **localhost-only by default**; protect with a strong shared secret header (`x-pallas-secret`).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.

## Disclaimer

Software is provided **as-is** for research and education. **Trading involves substantial risk of loss.** You are responsible for compliance with exchange rules and applicable law.
