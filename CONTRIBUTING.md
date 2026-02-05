# Contributing to Apilytics

Thank you for your interest in contributing to Apilytics!

## Branch Naming Convention

Use the following prefixes for branch names:

| Prefix | Purpose | Example |
|--------|---------|---------|
| `feature/` | New features or enhancements | `feature/variant-fallback` |
| `bugfix/` | Bug fixes | `bugfix/null-pointer-schema` |
| `hotfix/` | Urgent production fixes | `hotfix/auth-token-refresh` |
| `chore/` | Maintenance tasks (deps, CI, docs) | `chore/update-spark-version` |
| `refactor/` | Code refactoring without behavior change | `refactor/extract-schema-helper` |
| `test/` | Adding or updating tests | `test/wiremock-integration` |
| `docs/` | Documentation only changes | `docs/readme-examples` |

## Pull Request Guidelines

### PR Description Format

```markdown
Closes #XX

## Summary
Brief description of changes

## Test plan
- [ ] How to verify the changes work
```

Always put `Closes #XX` before the summary section.

### Commit Messages

- Use present tense ("Add feature" not "Added feature")
- Keep the first line under 72 characters
- Reference issues when relevant

## Code Style

- Follow existing patterns in the codebase
- Use meaningful variable and function names
- Keep functions focused and small
- Add scaladoc for public APIs

## Testing

- Add tests for new functionality
- Ensure all existing tests pass: `sbt test`
- E2E tests are skipped by default (set `E2E=1` to run)

## Development Setup

```bash
# Clone the repo
git clone https://github.com/Neutrinic/apilytics.git
cd apilytics

# Run tests
sbt test

# Run specific test suite
sbt "testOnly *SuiteName"
```
