# Credentials

Keep API tokens, passwords and client secrets out of config files that are committed to
version control. Config files support environment-variable substitution, so the secret
stays in the environment:

```hocon
auth {
  type = "bearer"
  token = ${API_TOKEN}         # resolved at load time from $API_TOKEN
}
```

Not this:

```hocon
auth {
  type = "bearer"
  token = "ghp_abc123secret"   # leaks into git history
}
```

`${VAR}` is required and fails if `VAR` is unset. `${?VAR}` is optional, and resolves to
nothing if unset.

## Authentication types

| `type` | Sends |
|---|---|
| `none` | Nothing. |
| `bearer` | `Authorization: Bearer <token>`. |
| `basic` | `Authorization: Basic`, from `username` and `password`. |
| `header` | A custom header, from `header-name` and `header-value`. |
| `oauth2_client` | A token from an OAuth2 client-credentials flow, using `client-id`, `client-secret` and `token-url`. It's fetched and refreshed for you. Keep both the client ID and the secret in environment variables. |

## Practices

- Export credentials in your shell, for example `export API_TOKEN=...`.
- Keep local config files that hold credentials out of git. `*.local.conf` is already in
  this repository's `.gitignore`.
- In CI, use the platform's secret store, such as GitHub Actions secrets or Vault.
- On a cluster, the config file is read and its variables resolved only on the driver.
  Executors receive the resolved values inside their tasks. So the variable is needed
  wherever the driver runs: in client deploy mode, that's the machine you submit from.
  In cluster deploy mode, set it with `spark.yarn.appMasterEnv.API_TOKEN` on YARN, or
  `spark.kubernetes.driverEnv.API_TOKEN` on Kubernetes.
