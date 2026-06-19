-- Benchmark: logical Yannakakis-style rewrite on composite-key join trees.
-- Run: build/Release/duckdb < benchmarks/bench_yannakakis_composite_keys.sql

.timer on
PRAGMA threads=8;

SELECT aggjoin_set_operator_enabled(false);
SELECT aggjoin_set_logical_rewrites_enabled(true);

.print === Composite-key chain, 100K key pairs per edge, 1M rows/table ===
CREATE TABLE yck0 AS
SELECT CAST(i % 1000 AS INTEGER) AS a,
       CAST(floor(i / 1000) % 100 AS INTEGER) AS b
FROM range(1000000) t(i);

CREATE TABLE yck1 AS
SELECT CAST(i % 1000 AS INTEGER) AS a,
       CAST(floor(i / 1000) % 100 AS INTEGER) AS b,
       CAST(i % 2000 AS INTEGER) AS c,
       CAST(floor(i / 2000) % 50 AS INTEGER) AS d
FROM range(1000000) t(i);

CREATE TABLE yck2 AS
SELECT CAST(i % 2000 AS INTEGER) AS c,
       CAST(floor(i / 2000) % 50 AS INTEGER) AS d
FROM range(1000000) t(i);

.print --- Logical rewrite ---
SELECT aggjoin_set_logical_rewrites_enabled(true);
SELECT aggjoin_reset_rewrite_marker();
SELECT COUNT(*) AS cnt
FROM yck0
JOIN yck1 ON yck0.a = yck1.a AND yck0.b = yck1.b
JOIN yck2 ON yck1.c = yck2.c AND yck1.d = yck2.d;
SELECT aggjoin_last_rewrite();

.print --- Native baseline ---
SELECT aggjoin_set_logical_rewrites_enabled(false);
SELECT aggjoin_reset_rewrite_marker();
SELECT COUNT(*) AS cnt
FROM yck0
JOIN yck1 ON yck0.a = yck1.a AND yck0.b = yck1.b
JOIN yck2 ON yck1.c = yck2.c AND yck1.d = yck2.d;
SELECT aggjoin_last_rewrite();

DROP TABLE yck0;
DROP TABLE yck1;
DROP TABLE yck2;

.print
.print === Mixed-width composite keys, 12K key pairs per edge, 240K rows/table ===
CREATE TABLE ycm0 AS
SELECT CAST(i % 300 AS SMALLINT) AS a,
       CAST(floor(i / 300) % 40 AS TINYINT) AS b
FROM range(240000) t(i);

CREATE TABLE ycm1 AS
SELECT CAST(i % 300 AS INTEGER) AS a,
       CAST(floor(i / 300) % 40 AS BIGINT) AS b,
       CAST(i % 400 AS SMALLINT) AS c,
       CAST(floor(i / 400) % 30 AS INTEGER) AS d,
       CAST(i % 997 AS BIGINT) AS v
FROM range(240000) t(i);

CREATE TABLE ycm2 AS
SELECT CAST(i % 400 AS BIGINT) AS c,
       CAST(floor(i / 400) % 30 AS SMALLINT) AS d
FROM range(240000) t(i);

.print --- Logical rewrite ---
SELECT aggjoin_set_logical_rewrites_enabled(true);
SELECT aggjoin_reset_rewrite_marker();
SELECT COUNT(*) AS cnt, SUM(ycm1.v) AS total
FROM ycm0
JOIN ycm1 ON ycm0.a = ycm1.a AND ycm0.b = ycm1.b
JOIN ycm2 ON ycm1.c = ycm2.c AND ycm1.d = ycm2.d;
SELECT aggjoin_last_rewrite();

.print --- Native baseline ---
SELECT aggjoin_set_logical_rewrites_enabled(false);
SELECT aggjoin_reset_rewrite_marker();
SELECT COUNT(*) AS cnt, SUM(ycm1.v) AS total
FROM ycm0
JOIN ycm1 ON ycm0.a = ycm1.a AND ycm0.b = ycm1.b
JOIN ycm2 ON ycm1.c = ycm2.c AND ycm1.d = ycm2.d;
SELECT aggjoin_last_rewrite();

DROP TABLE ycm0;
DROP TABLE ycm1;
DROP TABLE ycm2;

.print
.print === Low-fanout composite-key guard, 60K unique pairs ===
CREATE TABLE ycg0 AS
SELECT CAST(i AS INTEGER) AS a,
       CAST(0 AS INTEGER) AS b
FROM range(60000) t(i);

CREATE TABLE ycg1 AS
SELECT CAST(i AS INTEGER) AS a,
       CAST(0 AS INTEGER) AS b,
       CAST(i AS INTEGER) AS c,
       CAST(0 AS INTEGER) AS d
FROM range(60000) t(i);

CREATE TABLE ycg2 AS
SELECT CAST(i AS INTEGER) AS c,
       CAST(0 AS INTEGER) AS d
FROM range(60000) t(i);

.print --- Planner should stay native ---
SELECT aggjoin_set_logical_rewrites_enabled(true);
SELECT aggjoin_reset_rewrite_marker();
SELECT COUNT(*) AS cnt
FROM ycg0
JOIN ycg1 ON ycg0.a = ycg1.a AND ycg0.b = ycg1.b
JOIN ycg2 ON ycg1.c = ycg2.c AND ycg1.d = ycg2.d;
SELECT aggjoin_last_rewrite();

SELECT aggjoin_set_operator_enabled(true);
SELECT aggjoin_set_logical_rewrites_enabled(true);

DROP TABLE ycg0;
DROP TABLE ycg1;
DROP TABLE ycg2;
