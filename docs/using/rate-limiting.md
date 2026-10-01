# Rate limiting

```hocon
http {
  rate-limit = 10   # requests per second; omit for no limit
}
```

`rate-limit` is a **ceiling, not a target**. The limit is divided across partitions at
planning time, so 15 requests per second over 4 partitions becomes `4+4+4+3`, and each
partition throttles itself to its own share.

What that guarantees, and what it doesn't:

- **The limit is never exceeded.** The shares always add up to exactly the configured value.
- **It isn't coordinated across executors.** There's no shared token bucket; each
  partition simply obeys its share.
- **You may use less than you configured.** The division assumes every partition runs at
  once. If your cluster runs fewer tasks at a time than there are partitions, the real
  rate is proportionally lower: 10 partitions on 2 cores use roughly a fifth of the budget.
- **`rate-limit` must be at least the partition count**, because a partition can't be
  given less than 1 request per second. Planning fails with a clear message otherwise,
  rather than quietly exceeding the limit.

A true global limit under any cluster shape needs driver-coordinated permits, which is
tracked in [#205](https://github.com/Neutrinic/apilytics/issues/205).

Transient failures (429 and 5xx) are retried with exponential backoff, up to
`http.max-retries` attempts and `http.max-backoff` between them.
