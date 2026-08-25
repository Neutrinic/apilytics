-- Materialize five rows from PokeAPI into a view.
-- Downstream reads hit Parquet, not the API.
CREATE MATERIALIZED VIEW first_pokemon AS
SELECT name, url
FROM api.default.pokemon
LIMIT 5;
