-- Benchmark: logical Yannakakis-style rewrite on expression join keys.
-- Run: build/Release/duckdb < benchmarks/bench_yannakakis_expression_keys.sql

.timer on
PRAGMA threads=8;

SELECT aggjoin_set_operator_enabled(false);
SELECT aggjoin_set_logical_rewrites_enabled(true);

.print === Derived single-key guard, 600K rows/table ===
CREATE TABLE yek0 AS
SELECT i::INT AS id,
       (i % 11)::INT AS g,
       (i % 997)::INT AS v
FROM range(600000) t(i);

CREATE TABLE yek1 AS
SELECT (i + 1)::INT AS k,
       (i % 6000)::INT AS c,
       (i % 1009)::INT AS v1
FROM range(600000) t(i);

CREATE TABLE yek2 AS
SELECT ((i % 6000) + 2)::INT AS c_key,
       (i % 7)::INT AS g2,
       (i % 991)::INT AS v2
FROM range(600000) t(i);

.print --- Planner path ---
SELECT aggjoin_set_logical_rewrites_enabled(true);
SELECT aggjoin_reset_rewrite_marker();
SELECT yek0.g,
       yek2.g2,
       COUNT(*) AS c_all,
       SUM(yek0.v) AS s_v,
       SUM(yek1.v1) AS s_v1,
       MIN(yek2.v2) AS min_v2,
       MAX(yek2.v2) AS max_v2
FROM yek0
JOIN yek1 ON yek0.id + 1 = yek1.k
JOIN yek2 ON yek1.c = yek2.c_key - 2
GROUP BY yek0.g, yek2.g2;
SELECT aggjoin_last_rewrite();

.print --- Native baseline ---
SELECT aggjoin_set_logical_rewrites_enabled(false);
SELECT aggjoin_reset_rewrite_marker();
SELECT yek0.g,
       yek2.g2,
       COUNT(*) AS c_all,
       SUM(yek0.v) AS s_v,
       SUM(yek1.v1) AS s_v1,
       MIN(yek2.v2) AS min_v2,
       MAX(yek2.v2) AS max_v2
FROM yek0
JOIN yek1 ON yek0.id + 1 = yek1.k
JOIN yek2 ON yek1.c = yek2.c_key - 2
GROUP BY yek0.g, yek2.g2;
SELECT aggjoin_last_rewrite();

DROP TABLE yek0;
DROP TABLE yek1;
DROP TABLE yek2;

.print
.print === Projected derived-key guard, 600K rows/table ===
CREATE TABLE yep0 AS
SELECT i::INT AS id,
       (i % 11)::INT AS g,
       (i % 997)::INT AS v
FROM range(600000) t(i);

CREATE TABLE yep1 AS
SELECT (i + 1)::INT AS k,
       (i % 6000)::INT AS c,
       (i % 1009)::INT AS v1
FROM range(600000) t(i);

CREATE TABLE yep2 AS
SELECT ((i % 6000) + 2)::INT AS c_key,
       (i % 7)::INT AS g2,
       (i % 991)::INT AS v2
FROM range(600000) t(i);

.print --- Planner path ---
SELECT aggjoin_set_logical_rewrites_enabled(true);
SELECT aggjoin_reset_rewrite_marker();
SELECT p0.g,
       p2.g2,
       COUNT(*) AS c_all,
       SUM(p0.v) AS s_v,
       SUM(p1.v1) AS s_v1
FROM (SELECT id, id + 1 AS idp, g, v FROM yep0) p0
JOIN (SELECT k, c, v1 FROM yep1) p1 ON p0.idp = p1.k
JOIN (SELECT c_key - 2 AS c, g2, v2 FROM yep2) p2 ON p1.c = p2.c
GROUP BY p0.g, p2.g2;
SELECT aggjoin_last_rewrite();

.print --- Native baseline ---
SELECT aggjoin_set_logical_rewrites_enabled(false);
SELECT aggjoin_reset_rewrite_marker();
SELECT p0.g,
       p2.g2,
       COUNT(*) AS c_all,
       SUM(p0.v) AS s_v,
       SUM(p1.v1) AS s_v1
FROM (SELECT id, id + 1 AS idp, g, v FROM yep0) p0
JOIN (SELECT k, c, v1 FROM yep1) p1 ON p0.idp = p1.k
JOIN (SELECT c_key - 2 AS c, g2, v2 FROM yep2) p2 ON p1.c = p2.c
GROUP BY p0.g, p2.g2;
SELECT aggjoin_last_rewrite();

DROP TABLE yep0;
DROP TABLE yep1;
DROP TABLE yep2;

.print
.print === Composite expression keys, 240K rows/table ===
CREATE TABLE yec0 AS
SELECT CAST(i % 300 AS INTEGER) AS a,
       CAST(floor(i / 300) % 40 AS INTEGER) AS b,
       (i % 13)::INT AS g,
       (i % 997)::INT AS v
FROM range(240000) t(i);

CREATE TABLE yec1 AS
SELECT CAST((i % 300) + 1 AS INTEGER) AS ap,
       CAST((floor(i / 300) % 40) + 1 AS INTEGER) AS bp,
       CAST(i % 400 AS INTEGER) AS c,
       CAST(floor(i / 400) % 30 AS INTEGER) AS d,
       (i % 1009)::INT AS v1
FROM range(240000) t(i);

CREATE TABLE yec2 AS
SELECT CAST((i % 400) + 2 AS INTEGER) AS cp,
       CAST((floor(i / 400) % 30) + 3 AS INTEGER) AS dp,
       (i % 7)::INT AS g2,
       (i % 991)::INT AS v2
FROM range(240000) t(i);

.print --- Logical rewrite ---
SELECT aggjoin_set_logical_rewrites_enabled(true);
SELECT aggjoin_reset_rewrite_marker();
SELECT yec0.g,
       yec2.g2,
       COUNT(*) AS c_all,
       SUM(yec0.v) AS s_v,
       SUM(yec1.v1) AS s_v1,
       MIN(yec2.v2) AS min_v2,
       MAX(yec2.v2) AS max_v2
FROM yec0
JOIN yec1 ON yec0.a + 1 = yec1.ap AND yec0.b = yec1.bp - 1
JOIN yec2 ON yec1.c = yec2.cp - 2 AND yec1.d + 3 = yec2.dp
GROUP BY yec0.g, yec2.g2;
SELECT aggjoin_last_rewrite();

.print --- Native baseline ---
SELECT aggjoin_set_logical_rewrites_enabled(false);
SELECT aggjoin_reset_rewrite_marker();
SELECT yec0.g,
       yec2.g2,
       COUNT(*) AS c_all,
       SUM(yec0.v) AS s_v,
       SUM(yec1.v1) AS s_v1,
       MIN(yec2.v2) AS min_v2,
       MAX(yec2.v2) AS max_v2
FROM yec0
JOIN yec1 ON yec0.a + 1 = yec1.ap AND yec0.b = yec1.bp - 1
JOIN yec2 ON yec1.c = yec2.cp - 2 AND yec1.d + 3 = yec2.dp
GROUP BY yec0.g, yec2.g2;
SELECT aggjoin_last_rewrite();

DROP TABLE yec0;
DROP TABLE yec1;
DROP TABLE yec2;

.print
.print === Filtered projected aliases, currently cost-gated ===
CREATE TABLE yef0 AS
SELECT i::INT AS id,
       (i % 11)::INT AS g,
       (i % 997)::INT AS v
FROM range(600000) t(i);

CREATE TABLE yef1 AS
SELECT (i + 1)::INT AS k,
       (i % 6000)::INT AS c,
       (i % 1009)::INT AS v1
FROM range(600000) t(i);

CREATE TABLE yef2 AS
SELECT ((i % 6000) + 2)::INT AS c_key,
       (i % 7)::INT AS g2,
       (i % 991)::INT AS v2
FROM range(600000) t(i);

.print --- Planner path ---
SELECT aggjoin_set_logical_rewrites_enabled(true);
SELECT aggjoin_reset_rewrite_marker();
SELECT COUNT(*) AS cnt
FROM (SELECT id, id + 1 AS idp, g, v FROM yef0 WHERE id >= 0) p0
JOIN (SELECT k, c, v1 FROM yef1 WHERE k IS NOT NULL) p1 ON p0.idp = p1.k
JOIN (SELECT c_key - 2 AS c, g2, v2 FROM yef2 WHERE c_key IS NOT NULL) p2 ON p1.c = p2.c;
SELECT aggjoin_last_rewrite();

.print --- Native baseline ---
SELECT aggjoin_set_logical_rewrites_enabled(false);
SELECT aggjoin_reset_rewrite_marker();
SELECT COUNT(*) AS cnt
FROM (SELECT id, id + 1 AS idp, g, v FROM yef0 WHERE id >= 0) p0
JOIN (SELECT k, c, v1 FROM yef1 WHERE k IS NOT NULL) p1 ON p0.idp = p1.k
JOIN (SELECT c_key - 2 AS c, g2, v2 FROM yef2 WHERE c_key IS NOT NULL) p2 ON p1.c = p2.c;
SELECT aggjoin_last_rewrite();

DROP TABLE yef0;
DROP TABLE yef1;
DROP TABLE yef2;

SELECT aggjoin_set_operator_enabled(true);
SELECT aggjoin_set_logical_rewrites_enabled(true);
