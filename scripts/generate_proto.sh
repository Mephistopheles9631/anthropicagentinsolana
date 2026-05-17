#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 path/to/shredstream.proto"
  exit 1
fi

PROTO_FILE="$1"
OUT_DIR="src"

python -m grpc_tools.protoc \
  -I "$(dirname "$PROTO_FILE")" \
  --python_out="$OUT_DIR" \
  --grpc_python_out="$OUT_DIR" \
  "$PROTO_FILE"

echo "Generated protobuf stubs into $OUT_DIR"
