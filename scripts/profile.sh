#!/bin/bash
#
# Profiling script using cargo-flamegraph
# Usage: ./scripts/profile.sh <benchmark_name>
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$REPO_DIR"

if [ -z "$1" ]; then
    echo "Usage: $0 <benchmark_name>"
    echo ""
    echo "Available benchmarks:"
    echo "  - basic_ops"
    echo "  - concurrent"
    echo ""
    exit 1
fi

BENCH_NAME="$1"

echo "🔍 Profiling benchmark: $BENCH_NAME"
echo "======================================="
echo ""

# Check if flamegraph is installed
if ! command -v flamegraph &> /dev/null; then
    echo "Installing cargo-flamegraph..."
    cargo install flamegraph
fi

# Check OS-specific requirements
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "macOS detected. Make sure you have:"
    echo "  1. dtrace enabled: sudo dtraceSnoop"
    echo "  2. Or install cargo-instruments: cargo install cargo-instruments"
    echo ""
    echo "For better profiling on macOS, consider using cargo-instruments instead:"
    echo "  cargo instruments -t time --bench $BENCH_NAME"
    echo ""
fi

echo "Running profiling..."
cargo flamegraph --bench "$BENCH_NAME" --output "flamegraph-${BENCH_NAME}.svg"

echo ""
echo "✓ Profiling completed!"
echo "Flamegraph saved to: flamegraph-${BENCH_NAME}.svg"
echo "Open it with: open flamegraph-${BENCH_NAME}.svg"
