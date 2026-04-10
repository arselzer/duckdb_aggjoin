-- Benchmark: non-integral single-key follow-up
-- Integer single-key cases now mostly use direct or segmented-direct paths.
-- This benchmark keeps the join/group shape simple but uses DOUBLE keys to
-- track the current non-integral single-key strategy. On recent builds this
-- may rewrite to a native build-preaggregation plan rather than exercising
-- the older custom AGGJOIN hash path.
--
-- Native baseline uses the same query with the extension optimizer disabled so
-- both sides measure the same logical query shape.

.timer on

.print === Non-integral single-key follow-up (DOUBLE key), 100K keys, 1M rows ===
CREATE TABLE r_hd AS
SELECT CAST(i % 100000 AS DOUBLE) AS x,
       CAST(random() * 100 AS DOUBLE) AS val
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_hd AS
SELECT CAST(i % 100000 AS DOUBLE) AS x
FROM generate_series(1, 1000000) t(i);

.print --- AGGJOIN ---
COPY (
    SELECT r_hd.x, SUM(r_hd.val)
    FROM r_hd JOIN s_hd ON r_hd.x = s_hd.x
    GROUP BY r_hd.x
) TO '/tmp/aggjoin_hash_nonintegral_a.csv' (FORMAT CSV);

.print --- Native (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_hd.x, SUM(r_hd.val)
    FROM r_hd JOIN s_hd ON r_hd.x = s_hd.x
    GROUP BY r_hd.x
) TO '/tmp/aggjoin_hash_nonintegral_b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_hd;
DROP TABLE s_hd;
