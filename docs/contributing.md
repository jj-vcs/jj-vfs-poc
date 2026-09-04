# How to Contribute

We would love to accept your patches and contributions to this project.

---

## Development & Code Quality

Before submitting changes, ensure that your code adheres to the project's quality standards:

[Contributor License Agreement](https://cla.developers.google.com/about) (CLA).
You (or your employer) retain the copyright to your contribution; this simply
gives us permission to use and redistribute your contributions as part of the
project.

### 1. Code Formatting
Format your code using nightly `rustfmt`:
```bash
cargo +nightly fmt --all -- --check
```

### 2. Linting
Run Clippy without warnings:
```bash
cargo clippy --all-targets --all-features -- -D warnings
```

### 3. Testing
Verify that all unit and integration tests pass:
```bash
cargo test
```

---

## Contribution process

### Code Reviews

All submissions, including submissions by project members, require review. We
use [GitHub pull requests](https://docs.github.com/articles/about-pull-requests)
for this purpose.
