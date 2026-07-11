# Build This Repository Exactly

This is the single source of truth for recreating the repository. Build the project to match the current workspace exactly: same architecture, same file boundaries, same runtime behavior, same env contract, same startup flow, and same helper tooling.

You are an implementation agent. Recreate this repository as a production-grade, backend-only Solana mint intelligence pipeline. Preserve the current architecture, file layout, runtime behavior, and naming as closely as possible.

Do not diverge from the reference workspace. If something is present in the current repo, mirror it. If something is absent, do not invent it unless it is required to reproduce the current behavior exactly.

Canonical public repository: https://github.com/Mephistopheles9631/anthropicagentinsolana

When this prompt is shared on its own, the agent should use that public repository as the authoritative reference source. If a local checkout already exists, mirror its contents exactly. If not, retrieve the public repository first, then rebuild from that snapshot. Do not substitute generic Solana bot patterns for the repository contents.

## Goal

Build a Python application that:

- consumes Solana shredstream gRPC data in real time
- normalizes swap and pool-create events across multiple DEX programs
- stores raw and derived data in ClickHouse
- builds mint-level behavioral profiles
- optionally scores mints with Claude
- sends startup and opportunity notifications to Telegram and Discord
- enriches alerts with DexScreener market data
- uses an external Rust entry decoder for shred-only payloads

This is not a generic Solana bot. Recreate this exact repo structure and behavior.

## Required Stack

- Python 3.14+ in a virtual environment
- `grpcio`, `grpcio-tools`, `protobuf`
- `clickhouse-connect`
- `pydantic`, `pydantic-settings`
- `anthropic`
- `tenacity`
- `python-dotenv`
- `orjson`
- Rust Cargo build for the entry decoder helper

## Build Sequence

Follow this order when recreating the project:

1. Create the Python virtual environment and install `requirements.txt`.
2. Scaffold the top-level repo layout and package directories.
3. Add `docker-compose.yml`, `README.md`, `.env.example`, and `scripts/generate_proto.sh`.
4. Generate protobuf stubs into `src/`.
5. Add the Rust entry decoder under `tools/entry_decoder/` and verify it builds with `cargo build --release`.
6. Implement shared configuration in `src/app/config.py`.
7. Implement notifications in `src/app/notifications.py` with Telegram, Discord, and broadcast support.
8. Implement DexScreener enrichment in `src/app/dexscreener.py`.
9. Implement ClickHouse sinks and analytics buffering.
10. Implement the event model, program filtering, shredstream client, and IDL loading/validation.
11. Implement parser registry, DEX parsers, and instruction parsers.
12. Implement mint profiling in `src/app/profiler.py`.
13. Implement Claude scoring and opportunity alerting in `src/app/claude_scorer.py`.
14. Wire the orchestration flow in `src/app/main.py`.
15. Verify startup notifications, payload processing, ClickHouse writes, and alert delivery.

## Exact Reconstruction Rules

When using this prompt in a new workspace, the agent must treat the current repository snapshot as the source of truth and reconstruct the project from that snapshot, not from generic Solana-bot patterns.

- Inspect the full current file tree before writing code.
- Recreate the same module boundaries, file names, and directory layout.
- Prefer copying behavior and structure from the existing repo over inventing new abstractions.
- Keep the current runtime flow intact, including startup notification behavior, DexScreener enrichment, ClickHouse access patterns, and the Rust decoder integration.
- Preserve the current environment contract, including optional features, defaults, and disabled-by-default paths.
- If a file already exists in the reference workspace, mirror its behavior rather than replacing it with a simplified rewrite.
- Do not collapse multiple modules into one or split existing modules into additional layers unless the reference repo already does that.
- If implementation details are ambiguous, infer them from the current repository files before making new assumptions.
- The agent should verify the rebuilt project against the repository snapshot by checking that the same top-level files, package paths, and startup behavior are present.

## Reconstruction Checklist

Before writing code, the agent should confirm:

1. The same top-level files exist.
2. The same Python package paths exist under `src/app/`.
3. The same generated protobuf modules exist under `src/`.
4. The same Rust entry decoder exists under `tools/entry_decoder/`.
5. The same `idl/` contents are present.
6. The same startup flow is implemented in `src/app/main.py`.
7. The same notification and DexScreener behavior exists.
8. The same ClickHouse-backed profiling and scoring flow exists.
9. The same env variables are documented and consumed.
10. The rebuilt project can start with `PYTHONPATH=src python -m app.main`.

## Repository Shape

Recreate the same top-level layout:

- `README.md`
- `requirements.txt`
- `docker-compose.yml`
- `idl/`
- `scripts/`
- `src/`
- `tools/entry_decoder/`

Within `src/`, preserve the application package layout:

- `src/app/main.py`
- `src/app/config.py`
- `src/app/notifications.py`
- `src/app/claude_scorer.py`
- `src/app/profiler.py`
- `src/app/analytics_sink.py`
- `src/app/clickhouse_sink.py`
- `src/app/dexscreener.py`
- `src/app/entry_decoder.py`
- `src/app/idl_registry.py`
- `src/app/models.py`
- `src/app/pool_registry.py`
- `src/app/program_filter.py`
- `src/app/shredstream_client.py`
- `src/app/validation.py`
- `src/app/event_parsers/`
- `src/app/instruction_parsers/`

Also recreate the generated protobuf modules in `src/`:

- `*_pb2.py`
- `*_pb2_grpc.py`

## Functional Scope

### 1. Shredstream ingestion

- Connect to `SHREDSTREAM_GRPC_TARGET`.
- Load generated protobuf module and stub class names from env.
- Stream shredstream payloads continuously with reconnect behavior.
- Normalize payloads to JSON-like dictionaries for downstream processing.

### 2. Event parsing

- Use IDL-based parsing for multiple programs.
- Support at least these tracked program families:
  - Pump.fun
  - PumpSwap
  - Raydium CLMM / CPMM / LaunchLab
  - Meteora DAMM v2
  - Orca Whirlpool
- Build a parser registry that dispatches by program ID.
- Extract swap and create events.
- Track signer, slot, signature, and raw payload JSON.

### 3. ClickHouse storage

- Store raw filtered events.
- Maintain profile-ready derived tables and analytics buffers.
- Create schemas at startup if needed.
- Keep the application backend-only; do not add a UI.

### 4. Mint profiling

- Compute mint-level features over rolling windows like 5m, 15m, and 1h.
- Include metrics such as:
  - OHLCV-like price and volume aggregates
  - trade counts
  - buy/sell ratio
  - unique buyers
  - wallet concentration
  - creator history metrics
- Persist profiles in ClickHouse.

### 5. Claude scoring

- Score new mints against historical examples from ClickHouse.
- Use structured tool-output style responses.
- Deduplicate alerts within a configured window.
- Only send opportunity alerts when score and profile thresholds pass.
- If Claude is disabled by missing API key, ingestion and profiling still run.

### 6. Notifications

- Send a startup message when the app boots.
- Send opportunity alerts to Telegram and Discord.
- Use a shared broadcast helper so either or both destinations can be active.
- Keep alert output consolidated and readable.
- Include DexScreener context in alerts when available.

### 7. DexScreener enrichment

- Query DexScreener for token pair data.
- Pick the best Solana pair, preferring liquidity.
- Include price, liquidity, FDV, market cap, volume, txn counts, pair URL, dex ID, and pair address.
- If no market data exists yet, clearly indicate that it may be too new or not indexed.
- Preserve high precision for token price formatting so tiny prices remain visible.

### 8. Entry decoder helper

- Include a Rust helper at `tools/entry_decoder/`.
- Build it with `cargo build --release`.
- Use it to decode entry payloads when shredstream payloads contain transaction entries.
- The Python app should detect whether the decoder is available and log a warning if not.

## Environment Variables

Recreate the same `.env.example` contract with these key settings:

- `SHREDSTREAM_GRPC_TARGET`
- `GRPC_MESSAGES_MODULE`
- `GRPC_STUB_MODULE`
- `GRPC_STUB_CLASS`
- `GRPC_STREAM_METHOD`
- `GRPC_REQUEST_CLASS`
- `GRPC_REQUEST_JSON`
- `ENTRY_DECODER_PATH`
- `ENTRY_DECODER_TIMEOUT_SECONDS`
- `PROGRAM_ID_PUMPFUN`
- `PROGRAM_ID_PUMPSWAP`
- `PROGRAM_ID_RAYDIUM`
- `PROGRAM_IDS_RAYDIUM`
- `PROGRAM_ID_METEORA`
- `PROGRAM_IDS_METEORA`
- `PROGRAM_ID_METEORA_DAMM_V2`
- `PROGRAM_ID_WHIRLPOOL`
- `IDL_DIR`
- `IDL_PROGRAM_ID_MAP_JSON`
- `VALIDATE_IDLS_ON_STARTUP`
- `CLICKHOUSE_HOST`
- `CLICKHOUSE_PORT`
- `CLICKHOUSE_USERNAME`
- `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_DATABASE`
- `CLICKHOUSE_TABLE`
- `BATCH_SIZE`
- `FLUSH_INTERVAL_SECONDS`
- `PROFILE_DELAY_MINUTES`
- `PROFILE_MAX_AGE_MINUTES`
- `PROFILE_CHECK_INTERVAL_SECONDS`
- `ANTHROPIC_API_KEY`
- `CLAUDE_MODEL`
- `CLAUDE_SCORE_THRESHOLD`
- `CLAUDE_MAX_EXAMPLES`
- `CLAUDE_RETRY_ATTEMPTS`
- `CLAUDE_RETRY_MAX_WAIT_SECONDS`
- `CLAUDE_ALERT_DEDUP_MINUTES`
- `OPPORTUNITY_MIN_CONFIDENCE`
- `OPPORTUNITY_MIN_UNIQUE_BUYERS_5M`
- `OPPORTUNITY_MAX_WALLET_CONCENTRATION_5M`
- `DEXSCREENER_ENABLED`
- `DEXSCREENER_BASE_URL`
- `DEXSCREENER_TIMEOUT_SECONDS`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `DISCORD_WEBHOOK_URL`

## Startup Behavior

At startup, the app should:

- validate configured tracked program IDs
- load IDLs from `idl/`
- initialize ClickHouse schema
- bootstrap pool registry state from ClickHouse
- build parser registry
- initialize the Claude scorer if enabled
- start the mint profiler task
- initialize the entry decoder
- send a startup notification to Telegram and Discord if configured

## Logging and Operational Expectations

- Use structured INFO logs for major lifecycle events.
- Log parser readiness and startup state.
- Log notification send success/failure.
- Log dex enrichment failures without crashing the process.
- Serialize ClickHouse access so concurrent queries do not run in the same session.

## Acceptance Criteria

The build is correct only if the recreated app can do all of the following:

- start with `PYTHONPATH=src python -m app.main`
- connect to shredstream and process payloads
- parse supported programs through the registry
- store events and profiles in ClickHouse
- compute mint profiles and score them with Claude when enabled
- send startup notifications to Telegram and Discord
- send opportunity alerts to Telegram and Discord
- enrich opportunity alerts with DexScreener data
- handle missing market data gracefully
- keep the entry decoder as a separate Rust helper

## Implementation Notes

- Keep the code backend-only; do not introduce a frontend.
- Match the current module names and file boundaries rather than collapsing everything into fewer files.
- Preserve the existing prompt-oriented behavior of the scorer and notifications.
- Favor straightforward, production-oriented code over clever abstractions.
- If you regenerate protobufs, place them under `src/` as this repo expects.

## Local Runbook

1. Create a venv.
2. Install `requirements.txt`.
3. Generate protobuf stubs from the provided shredstream proto.
4. Build `tools/entry_decoder` with Cargo release mode.
5. Copy `.env.example` to `.env` and fill in the required values.
6. Start ClickHouse with Docker Compose.
7. Run the app with `PYTHONPATH=src python -m app.main`.

## Rebuild Expectations

An agent following this prompt should be able to rebuild the repository exactly as it exists now, including:

- the current backend-only structure
- the current startup notification behavior
- the current Telegram and Discord broadcast flow
- the current DexScreener price formatting precision
- the current Rust entry decoder integration
- the current ClickHouse-backed profiling and scoring pipeline
- the current parser registry and tracked program set

Do not simplify the project into fewer files or a different architecture. Match the current module boundaries and behavior.

## Final Output

Deliver a repository that matches the current one in structure, behavior, and operational flow, including the notification pipeline, profiling pipeline, program support, and helper tooling.