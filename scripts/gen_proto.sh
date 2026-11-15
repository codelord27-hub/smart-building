#!/usr/bin/env bash
# Generates Python gRPC stubs for the 2PC protocol into services/common/sbproto/.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
python -m grpc_tools.protoc -I "$ROOT/proto" \
  --python_out="$ROOT/services/common" \
  --grpc_python_out="$ROOT/services/common" \
  "$ROOT/proto/sbproto/twopc.proto"
echo "generated: services/common/sbproto/twopc_pb2.py, twopc_pb2_grpc.py"
