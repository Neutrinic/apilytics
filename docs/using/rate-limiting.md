# Rate limiting

```hocon
http {
  rate-limit = 10   # requests per second; omit for no limit
}
```

`rate-limit` is a **budget for one scan**, divided between its partitions. At planning
time, 15 requests per second over 4 partitions becomes `4+4+4+3`, and each partition
throttles itself to its own share.

What that guarantees, and what it doesn't:

- **A scan stays within the limit, retries included.** The shares add up to exactly the
  configured value, and every attempt takes a permit: retries after a 429, a 5xx or a
  network error wait their turn like any other request.
- **Concurrent scans each get the whole budget.** Two queries reading the same API at once
  can together send twice the limit.
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

Transient failures (429 and 5xx) are retried with exponential backoff: up to
`http.max-retries` retries after the first request, with at most `http.max-backoff` between
them. A 429 that says how long to wait, with `Retry-After` in seconds or as a date, is
retried after exactly that, even beyond `max-backoff`: retrying sooner would only draw
another 429. A `Retry-After` that can't be read, or is too large to wait for, falls back to
the backoff.
