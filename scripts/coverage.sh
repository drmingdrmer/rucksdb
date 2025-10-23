#!/bin/bash
# Code coverage tool using cargo-tarpaulin
# This is a guidance tool to understand test coverage, not required for CI

set -e

echo "📊 Running code coverage analysis..."

# Check if tarpaulin is installed
if ! command -v cargo-tarpaulin &> /dev/null; then
    echo "cargo-tarpaulin not found. Installing..."
    cargo install cargo-tarpaulin
fi

# Run coverage
echo "Generating coverage report..."
cargo tarpaulin --verbose --all-features --workspace --timeout 120 --out Html --out Xml

echo ""
echo "✅ Coverage analysis complete!"
echo ""
echo "Reports generated:"
echo "  - HTML: tarpaulin-report.html (open in browser)"
echo "  - XML:  cobertura.xml (for CI tools)"
echo ""
echo "💡 Note: Coverage is a guidance tool. Focus on meaningful test cases,"
echo "   not just achieving high coverage percentages."
