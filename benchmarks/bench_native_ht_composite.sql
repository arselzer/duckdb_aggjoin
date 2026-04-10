-- Benchmark: selective native GroupedAggregateHashTable path inside AGGJOIN
-- Shape: composite join/group key, SUM only, no build-side aggregates.
-- This is intended to hit AGGJOIN's internal use_native_ht path.

.timer on

.print === Selective native-HT composite path, 2-column key, 1M rows ===
CREATE TABLE r_nh AS
SELECT i % 1000 AS x,
       i % 100 AS y,
       CAST(random() * 100 AS DOUBLE) AS val
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_nh AS
SELECT i % 1000 AS x,
       i % 100 AS y
FROM generate_series(1, 1000000) t(i);

.print --- AGGJOIN / native-HT path ---
COPY (
    SELECT r_nh.x, r_nh.y, SUM(r_nh.val)
    FROM r_nh JOIN s_nh ON r_nh.x = s_nh.x AND r_nh.y = s_nh.y
    GROUP BY r_nh.x, r_nh.y
) TO '/tmp/aggjoin_native_ht_composite_a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_nh.x, r_nh.y, SUM(r_nh.val)
    FROM r_nh JOIN s_nh ON r_nh.x = s_nh.x AND r_nh.y = s_nh.y
    GROUP BY r_nh.x, r_nh.y
) TO '/tmp/aggjoin_native_ht_composite_b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_nh;
DROP TABLE s_nh;
