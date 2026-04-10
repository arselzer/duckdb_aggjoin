-- Benchmark: sparse low-fanout shape that the planner intentionally bails to native.
-- This validates the sparse gate on the direct query shape.

.timer on

.print === Sparse low-fanout planner-gate shape, 100K rows over 10M range ===
CREATE TABLE r_sg AS
SELECT CAST(random() * 10000000 AS INT) AS x,
       CAST(random() * 100 AS DOUBLE) AS val
FROM generate_series(1, 100000) t(i);

CREATE TABLE s_sg AS
SELECT x FROM r_sg;

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_sg.x, SUM(r_sg.val)
    FROM r_sg JOIN s_sg ON r_sg.x = s_sg.x
    GROUP BY r_sg.x
) TO '/tmp/aggjoin_sparse_gate_a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_sg.x, SUM(r_sg.val)
    FROM r_sg JOIN s_sg ON r_sg.x = s_sg.x
    GROUP BY r_sg.x
) TO '/tmp/aggjoin_sparse_gate_b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_sg;
DROP TABLE s_sg;
