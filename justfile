fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

lint:
    cargo clippy --all-targets --all-features -- -D warnings

fix-lint:
    cargo clippy --fix --all-targets --all-features --allow-dirty --allow-staged

check: fmt-check lint
