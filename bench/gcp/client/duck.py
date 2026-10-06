"""DuckDB baseline: read_json over every offset page URL, summing the same columns as apilytics.

usage: duck.py <base-url> <threads>
Prints one tab-separated line: wall seconds, CPU seconds, then count and sums.
"""
import duckdb, json, resource, sys, time, urllib.request

base, threads = sys.argv[1], int(sys.argv[2])
rows = json.load(urllib.request.urlopen(base + "/manifest.json"))["row_count"]
urls = [f"{base}/offset?offset={o}&limit=5000" for o in range(0, rows, 5000)]

con = duckdb.connect()
con.execute(f"SET threads = {threads}")
con.execute("INSTALL httpfs; LOAD httpfs")
q = """SELECT count(*), sum(r.benchmark_id), round(sum(r.trip_distance), 2), round(sum(r.fare_amount), 2)
       FROM (SELECT unnest(results) AS r FROM read_json($urls,
             columns = {results: 'STRUCT(benchmark_id BIGINT, trip_distance DOUBLE, fare_amount DOUBLE)[]'}))"""
before = resource.getrusage(resource.RUSAGE_SELF)
t = time.time()
res = con.execute(q, {"urls": urls}).fetchone()
wall = time.time() - t
after = resource.getrusage(resource.RUSAGE_SELF)
cpu = (after.ru_utime + after.ru_stime) - (before.ru_utime + before.ru_stime)
print("\t".join([f"{wall:.1f}", f"{cpu:.1f}"] + [str(v) for v in res]))
