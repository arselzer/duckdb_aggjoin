-- Split benchmark from bench.sql
-- 5. Sparse keys: 100K rows over a 10M range, AGGJOIN side only
-- Run: build/Release/duckdb < benchmarks/bench_core_sparse_aggjoin.sql

.print === Sparse: ~100K keys, range 10M, 100K rows (AGGJOIN only) ===
.timer on

CREATE TABLE r5 AS SELECT CAST(random() * 10000000 AS INT) AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,100000) t(i);
CREATE TABLE s5 AS SELECT x FROM r5;

.print --- AGGJOIN (hash mode) ---
COPY (SELECT r5.x, SUM(r5.val) FROM r5 JOIN s5 ON r5.x = s5.x GROUP BY r5.x) TO '/tmp/aggjoin_bench_5a.csv' (FORMAT CSV);

DROP TABLE r5;
DROP TABLE s5;
