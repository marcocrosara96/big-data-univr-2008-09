-- 1.2 Exercise 2
-- Problem statement: count the total number of TCP connection having “akamai” in the “SSL Server Hello” field (#111
-- in the tstat data).
-- Hint: You need to modify the code of the previous exercise, filtering the loaded data, and applying a different grouping.
-- Questions: 1. How many reducers were launched? Can you increase the number of reducers?

-- A is in ex1

B = FILTER A BY ssl_server_hello matches '.*akamai.*';
C = GROUP B ALL; -- devo farlo poichè COUNT lavora su campi bag
D = FOREACH C GENERATE group, COUNT(B);
DUMP D;