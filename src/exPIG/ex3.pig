-- 1.3 Exercise 3
-- Problem statement: for each client IP, compute the sum of uploaded, downloaded and total (up+down) transmitted
-- bytes.

B = FOREACH A GENERATE ip_c, unique_bytes_c, unique_bytes_s, unique_bytes_c + unique_bytes_s;
C = GROUP B BY ip_c;
D = FOREACH C GENERATE group, SUM(B.unique_bytes_c), SUM(B.unique_bytes_s), SUM(B.$3);
DUMP D;