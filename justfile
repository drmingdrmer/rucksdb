# RucksDB Development Tasks
# https://github.com/casey/just

# List all available tasks
default:
    @just --list

# Run all CI checks locally (format, clippy, tests)
ci:
    @./scripts/ci.sh

# Check code formatting
fmt:
    cargo fmt --all

# Check code formatting (without modifying)
fmt-check:
    cargo fmt --all -- --check

# Run clippy linter
clippy:
    cargo clippy --all-targets --all-features -- -D warnings

# Run all tests
test:
    cargo test --all-features

# Run tests with output
test-verbose:
    cargo test --all-features -- --nocapture

# Run a specific test
test-one TEST:
    cargo test {{TEST}} -- --nocapture

# Run all benchmarks
bench:
    @./scripts/bench.sh

# Run a specific benchmark
bench-one NAME:
    @./scripts/bench.sh {{NAME}}

# Generate flamegraph for performance profiling
profile NAME:
    @./scripts/profile.sh {{NAME}}

# Generate code coverage report (HTML + XML)
coverage:
    @./scripts/coverage.sh

# Build the project
build:
    cargo build

# Build with optimizations
build-release:
    cargo build --release

# Clean build artifacts
clean:
    cargo clean

# Update dependencies
update:
    cargo update

# Check for outdated dependencies
outdated:
    cargo outdated

# Install development tools
install-tools:
    cargo install cargo-tarpaulin
    cargo install cargo-flamegraph
    cargo install cargo-outdated

# Run the example
example:
    cargo run --example basic

# Generate and open documentation
doc:
    cargo doc --no-deps --open

# Check project for common issues
audit:
    cargo audit

# Format, lint, and test (pre-commit check)
pre-commit: fmt clippy test
    @echo "✅ All pre-commit checks passed!"
