# Privacy

## APIlytics sends nothing on its own

APIlytics contains no usage reporting. The only network calls it makes are the ones your
configuration describes: to the OpenAPI spec's location, to the OAuth2 token endpoint if
you use one, and to the API you query. There is nothing to opt out of.

## What the API sees

The API you query receives what your configuration and queries send it:

- your credentials
- the query parameters that pushed-down filters become
- one request per page, and per parent row for [parent-child](using/joins.md) tables

APIlytics' errors name the host and path of a failed request, never its query string,
because a query string can carry credentials.

## This site counts page views

Each page of this site loads a one-pixel image from [Scarf](https://about.scarf.sh/),
which counts page views for the maintainers. It sets no cookies. Scarf uses the request's
IP address to look up the organisation it belongs to, then discards it, and uses the
referrer to tell which page was viewed. The README on GitHub carries a pixel of its own.

## Download statistics

Downloads from Maven Central are counted from Maven Central's own download data, which
Sonatype shares with Scarf for maintainers. That's data about requests to Maven Central,
not something APIlytics collects: the jar does nothing at download or at run time to
report it.
