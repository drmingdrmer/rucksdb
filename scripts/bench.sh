#!/bin/bash
#
# Benchmark runner script
# Usage: ./scripts/bench.sh [benchmark_name]
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$REPO_DIR"

echo "🚀 Running RucksDB Benchmarks"
echo "=============================="

if [ -z "$1" ]; then
    echo ""
    echo "Running all benchmarks..."
    cargo bench --benches
else
    echo ""
    echo "Running benchmark: $1"
    cargo bench --bench "$1"
fi

echo ""
echo "✓ Benchmarks completed!"
echo ""
echo "Results are saved in target/criterion/"
echo "View HTML reports: open target/criterion/report/index.html"
