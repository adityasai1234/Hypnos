#!/bin/bash
# Provisional measurement run. Numbers from this container do not close the gate.
set -euo pipefail
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/opt/target}"
cd /src
cargo build -p hypnos-guest --target wasm32-wasip1 --release
cargo build -p spike --release
echo "=== cross check ==="
cargo check --target x86_64-unknown-linux-gnu -p spike
WASM="$CARGO_TARGET_DIR/wasm32-wasip1/release/hypnos_guest.wasm"
SPIKE="$CARGO_TARGET_DIR/release/spike"
ROOT=/tmp/hypnos
"$SPIKE" sysinfo
echo "=== compile ==="
"$SPIKE" compile "$WASM" -o /tmp/guest.cwasm
"$SPIKE" compile --target x86_64 "$WASM" -o /tmp/guest.x64.cwasm
echo "=== trap ==="
"$SPIKE" trap --wasm "$WASM" --script scripts/counter.js --root "$ROOT/trap"
echo "=== run ==="
printf 'reload\n' | "$SPIKE" run --wasm "$WASM" --script scripts/counter.js --root "$ROOT/run"
echo "=== agent ==="
"$SPIKE" agent --wasm "$WASM" --script scripts/agent.js --root "$ROOT/agent" --delay-ms 5000
echo "=== wake ==="
"$SPIKE" wake-bench --wasm "$WASM" --script scripts/counter.js --root "$ROOT/wake" --iters 1000 --cold-iters 30
"$SPIKE" wake-bench --wasm "$WASM" --script scripts/counter.js --root "$ROOT/wake-nm" --iters 1000 --cold-iters 30 --no-meter
echo "=== crash ==="
"$SPIKE" crash --wasm "$WASM" --script scripts/counter.js --root "$ROOT/crash" --fail-each 100 --kills 500
echo "=== churn ==="
"$SPIKE" churn --wasm "$WASM" --script scripts/counter.js --root "$ROOT/churn" --actors 5000 --rounds 5
echo "=== boot-scan ==="
"$SPIKE" boot-scan --root "$ROOT/boot" --n 1000,10000,100000
echo "=== SUITE DONE ==="
