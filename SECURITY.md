# Security Policy

## Reporting a vulnerability

Please report security issues privately through GitHub's
[security advisory](https://github.com/khangpt2k6/Slipstream_CDC/security/advisories/new)
form rather than opening a public issue. Include the affected version or commit,
steps to reproduce, and the impact you observed. Expect an initial response
within a few days.

## Automated scanning

Every push and pull request runs
[.github/workflows/security.yml](.github/workflows/security.yml):

| Check | Scope | Gates the build |
| ----- | ----- | --------------- |
| `govulncheck` | Go standard library and dependencies our code calls | Yes |
| `gosec` | static analysis of our Go code | Yes |
| `gitleaks` | committed secrets across the history | Yes |
| `dependency-review` | new vulnerable or incompatibly licensed deps on a PR | Yes |
| CodeQL | semantic analysis of the Go code | Reports to Security tab |
| Trivy | vulnerable deps and misconfigured compose | Reports to Security tab |

Dependency and GitHub Actions updates arrive automatically through
[Dependabot](.github/dependabot.yml).

## Scope notes

The committed `cdc/cdc` credentials and DSNs are local-only dev defaults for the
docker-compose stack on `localhost`. They are not real secrets and are
allowlisted in [.gitleaks.toml](.gitleaks.toml). Do not use them outside local
development.
