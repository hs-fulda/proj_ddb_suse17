set echo on;
alter session set nls_language = english;
alter session set nls_date_format = 'DD-MON-YYYY';
alter session set nls_date_language = english;

-- CREATE table BUCHUNG (tid integer);
-- CREATE table PASSAGIER (tid integer);
-- CREATE table FLUG (tid integer);
-- CREATE table FLUGLINIE (tid integer);
-- CREATE table FLUGHAFEN (tid integer);
-- CREATE table DB04_Colors(tid integer);
-- CREATE table part_tab(tid integer);


drop table BUCHUNG cascade constraints;

drop table PASSAGIER cascade constraints;

drop table FLUG cascade constraints;

drop table FLUGLINIE cascade constraints;

drop table FLUGHAFEN cascade constraints;
--author: Aleks A.
DROP table DB04_Colors;
DROP TABLE part_tab;
-- This will output a list of available tables:
SELECT table_name, status, num_rows FROM USER_TABLES;