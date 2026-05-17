# Solana Mint Intelligence Bot

Production-grade Solana trading intelligence pipeline that consumes shredstream gRPC data, normalizes swap/create events across multiple DEX programs, and builds mint-level behavioral profiles in ClickHouse. A Claude-based scorer classifies new mints against historical patterns and can alert via Telegram when a high-confidence opportunity is detected.

## Capabilities

- Real-time shredstream ingestion with auto-reconnect
- Multi-program parsing with IDL-based discriminators
- Mint-level price and volume aggregation (OHLCV)
- Behavioral profiling windows (5m/15m/1h) with wallet concentration metrics
- Claude AI scoring with structured output and historical examples
- Telegram alerts for high-score mints and startup health

## Supported programs

- Pump.fun
- PumpSwap
- Raydium CLMM / CPMM / LaunchLab
- Meteora DAMM v2
- Orca Whirlpool

## Architecture (high level)

1) Shredstream gRPC -> payload normalization
2) Protocol parsers -> swaps / pool creates
3) ClickHouse storage + materialized views
4) Mint profiler -> behavioral features
5) Claude scorer -> mint_scores + Telegram alerts

## Setup

### 1) Create environment

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2) Generate gRPC stubs

```bash
chmod +x scripts/generate_proto.sh
./scripts/generate_proto.sh /absolute/path/to/shredstream.proto
```

This generates `src/shredstream_pb2.py` and `src/shredstream_pb2_grpc.py`.

### 3) Configure environment

```bash
cp .env.example .env
```

Edit `.env` and set:

- gRPC target (`SHREDSTREAM_GRPC_TARGET`)
- Program IDs and IDL map
- ClickHouse connection
- Claude API key (optional)
- Telegram bot token + chat id (optional)

### 4) Start ClickHouse

Use your local ClickHouse service or a containerized instance.

### 5) Run

```bash
PYTHONPATH=src python -m app.main
```

## Telegram alerts

- Startup: a “bot started” message is sent when Telegram is enabled.
- High-score mint: when Claude score >= `CLAUDE_SCORE_THRESHOLD`, a Telegram alert is sent.

Configure in `.env`:

```
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
```

## Data tables

The pipeline builds and maintains:

- `shredstream_events` (raw stream)
- `swaps`, `pool_creates`
- `mint_ohlcv` (via materialized view)
- `mint_profiles`, `creator_profiles`
- `mint_scores`

## Operational notes

- Missing IDLs can be bypassed by setting `VALIDATE_IDLS_ON_STARTUP=false`.
- If you change proto names, update `GRPC_*` in `.env`.
- Claude scoring is optional; the pipeline runs without it.
