# Security policy

## Reporting a vulnerability

Please don't open a public issue for a security problem. Report it privately through
GitHub: on the repository's **Security** tab, choose **Report a vulnerability**, or go
straight to <https://github.com/Neutrinic/apilytics/security/advisories/new>. Only the
maintainers can see the report.

Include what you can of:

- the version of APIlytics and of Spark, and where it runs (spark-shell, a cluster,
  Databricks, the Docker image)
- the config, with credentials removed
- what an attacker could do, and the steps that show it

APIlytics is maintained by volunteers, so there's no guaranteed response time, but reports
are dealt with before other work. You'll hear back on the report itself, and you'll be
credited in the advisory unless you'd rather not be.

## What happens next

A confirmed vulnerability is fixed in a patch release of each supported version. A GitHub
security advisory is then published with the fix, describing the problem, the versions
affected and how to upgrade, and the changelog lists the fix under **Security**.

## Supported versions

| Version | Supported |
|---|---|
| 1.0.x | Yes |
| 0.x | No: upgrade to 1.0 |

## Scope

In scope is anything APIlytics itself does: the connector jar, the Docker image
`ghcr.io/neutrinic/apilytics`, and the example configs it ships. For example:

- credentials, tokens or secrets reaching logs, error messages or the Spark UI
- values from an API response or a config changing which URL or host a request goes to
- credentials sent somewhere they weren't configured for, or over plaintext without the
  warning APIlytics gives

Out of scope:

- vulnerabilities in Apache Spark, the JVM, or a library APIlytics depends on, unless
  APIlytics' use of it is what makes them exploitable. Report those upstream. Dependencies
  are scanned in CI, and a known vulnerability in one that ships in the jar or the image is
  welcome as an ordinary issue.
- the APIs you point APIlytics at, and what they return
- a config file's own secrets being readable by whoever can read the file
