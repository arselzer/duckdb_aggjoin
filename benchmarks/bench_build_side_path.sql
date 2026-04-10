-- Benchmark: build-side aggregate shape that still fires AGGJOIN after planner swap
-- Shape: probe groups by join key, build contributes SUM/COUNT/AVG/MIN/MAX.

.timer on

.print === Build-side aggregate path, 100K keys, 1M rows ===
CREATE TABLE r_bs AS
SELECT i % 100000 AS x
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_bs AS
SELECT i % 100000 AS x,
       CAST(random() * 100 AS DOUBLE) AS y
FROM generate_series(1, 1000000) t(i);

.print --- AGGJOIN build-side path ---
COPY (
    SELECT r_bs.x,
           SUM(s_bs.y),
           COUNT(s_bs.y),
           AVG(s_bs.y),
           MIN(s_bs.y),
           MAX(s_bs.y)
    FROM r_bs JOIN s_bs ON r_bs.x = s_bs.x
    GROUP BY r_bs.x
) TO '/tmp/aggjoin_build_side_path_a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_bs.x,
           SUM(s_bs.y),
           COUNT(s_bs.y),
           AVG(s_bs.y),
           MIN(s_bs.y),
           MAX(s_bs.y)
    FROM r_bs JOIN s_bs ON r_bs.x = s_bs.x
    GROUP BY r_bs.x
) TO '/tmp/aggjoin_build_side_path_b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_bs;
DROP TABLE s_bs;
