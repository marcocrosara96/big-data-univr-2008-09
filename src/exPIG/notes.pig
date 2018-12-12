-- Per lanciare pig in locale e non su hdfs --> pig -x local

proj_A = FOREACH A GENERATE ip_c, port_c,  data_bytes_c, ip_s, port_s, data_bytes_s;
DUMP proj_A; --forzo pig a visualizzare

sel_A = FILTER proj_A BY port_c != 5590;
DUMP sel_A;
DESCRIBE sel_A;

group_A = GROUP sel_A BY ip_c; -- Dato un indirizzo ip, mostra la bag con le tuple che hanno quell'indirizzo ip
DUMP group_A;
DESCRIBE group_A; --interessanti gli identificatori 'group:' e 'sel_A:'

--Vogliamo contare quante tuple ci sono dentro a ciascuna bag per ogni indirizzo ip
count_A = FOREACH group_A GENERATE group, COUNT(sel_A);
DUMP count_A;

--ESEMPIO : per ogni IP contare # connessioni
--1 -> PIG appena visto ...
--              A = LOAD ...
--              B = GROUP A BY IP
--              FOREAC B GENERATE group, COUNT(A)
--2 -> MAP_REDUCE -> è un po' più oneroso
--              EMIT(ip_c, 1);
--              EMIT(ip_c, sum)