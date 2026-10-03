# Governance

APIlytics is a single-maintainer project. This page says who decides what, and how that can
change.

## Maintainers

| Maintainer | Role |
|---|---|
| Aghmat Abrahams ([@Neutrinic](https://github.com/Neutrinic)) | Lead maintainer |

The lead maintainer reviews and merges pull requests, triages issues, cuts releases, and
handles security reports (see [SECURITY.md](SECURITY.md)).

## How decisions are made

Changes are proposed in issues and made in pull requests, as described in
[CONTRIBUTING.md](CONTRIBUTING.md): every pull request has an issue, and is reviewed before
it merges. Anyone can open either, and discussion happens there, in the open.

The lead maintainer makes the final call, including on what's in scope for the project and
when a release is ready. A decision that isn't obvious from the code is explained in the
issue or pull request, so the reasoning is on record.

## Releases

Releases follow [semantic versioning](https://semver.org): a breaking change to the
config, the SQL a table exposes, or the published coordinate needs a new major version.
Every user-visible change is listed in [CHANGELOG.md](CHANGELOG.md). Releases are
published to Maven Central from a tag, together with the Docker image and the docs site.

## Becoming a maintainer

Contributors who have made sustained, careful contributions, through code, reviews or
issue triage, may be invited by the lead maintainer to become maintainers. A new
maintainer is added to the table above in a pull request. If the project grows to several
maintainers, this page will be updated to say how they decide together.
