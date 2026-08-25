-- A materialized view over a REST API.
--
-- `api.default.pokemon` is served by APIlytics from PokeAPI's OpenAPI spec.
-- SDP materializes the result into the pipeline's storage location, so
-- downstream reads hit Parquet rather than the API.
CREATE MATERIALIZED VIEW first_pokemon AS
SELECT name, url
FROM api.default.pokemon
LIMIT 5;
