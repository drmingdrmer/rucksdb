#!/bin/bash
#
# Local CI script - runs all checks before pushing
# Usage: ./scripts/ci.sh
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$REPO_DIR"

echo "🔄 Running Local CI Checks"
echo "=========================="
echo ""

# 1. Format check
echo "1️⃣  Checking code formatting..."
if ! cargo fmt --all -- --check; then
    echo ""
    echo "❌ Formatting check failed!"
    echo "Run: cargo fmt --all"
    exit 1
fi
echo "✓ Formatting check passed"
echo ""

# 2. Clippy
echo "2️⃣  Running clippy..."
if ! cargo clippy --all-targets --all-features -- -D warnings; then
    echo ""
    echo "❌ Clippy check failed!"
    exit 1
fi
echo "✓ Clippy check passed"
echo ""

# 3. Tests
echo "3️⃣  Running tests..."
if ! cargo test --all-features; then
    echo ""
    echo "❌ Tests failed!"
    exit 1
fi
echo "✓ All tests passed"
echo ""

# 4. Doc tests
echo "4️⃣  Running doc tests..."
if ! cargo test --doc; then
    echo ""
    echo "❌ Doc tests failed!"
    exit 1
fi
echo "✓ Doc tests passed"
echo ""

# 5. Build check
echo "5️⃣  Checking build..."
if ! cargo build --all-features; then
    echo ""
    echo "❌ Build failed!"
    exit 1
fi
echo "✓ Build passed"
echo ""

echo "=============================="
echo "✅ All CI checks passed!"
echo ""
echo "You can now safely push your changes:"
echo "  git push"
