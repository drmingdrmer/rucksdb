#!/bin/bash
#
# Install git hooks for this repository
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
HOOKS_DIR="$REPO_DIR/.git/hooks"

echo "Installing git hooks..."

# Copy pre-commit hook
cp "$SCRIPT_DIR/pre-commit" "$HOOKS_DIR/pre-commit"
chmod +x "$HOOKS_DIR/pre-commit"

echo "✓ Git hooks installed successfully!"
echo ""
echo "The pre-commit hook will now run:"
echo "  1. cargo fmt --check"
echo "  2. cargo clippy"
echo "  3. cargo test"
echo ""
echo "To skip the pre-commit hook, use: git commit --no-verify"
