# APIlytics

> Turn any REST API into a SQL-queryable data source in minutes.

⚠️ **Work in Progress** - This is a prototype. Come back in a few weeks!

## What is this?

APIlytics is an Apache Spark catalog that automatically generates tables from OpenAPI specifications, allowing you to query REST APIs using SQL.
```python
# Point to an OpenAPI spec
spark.catalog.loadAPI(
    name="myapi",
    spec="https://api.example.com/openapi.yaml"
)

# Query with SQL
spark.sql("SELECT * FROM myapi.users WHERE status = 'active'").show()
```

## Features (Planned)

- [x] Parse OpenAPI specs
- [ ] Generate Spark schemas automatically
- [ ] Query APIs with SQL
- [ ] Smart caching with Delta Lake
- [ ] Apache Arrow for 10-100x performance
- [ ] Join across multiple APIs
- [ ] Intelligent batching and pagination

## Development
```bash
# Build
sbt compile

# Test
sbt test

# Run example
sbt run
```

## Roadmap

**Week 1-2:** Core Spark catalog + OpenAPI parsing
**Week 3-4:** HTTP fetching + basic queries working
**Week 5-6:** Performance optimizations (Arrow, caching)

## License

Apache License 2.0 - Same as Apache Spark

---

Built with ❤️ by engineers who've felt the pain of manual API integration.