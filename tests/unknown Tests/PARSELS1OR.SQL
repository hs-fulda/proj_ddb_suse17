set echo on;

alter session set nls_language = english;
alter session set nls_date_format = 'DD-MON-YYYY';
alter session set nls_date_language = english;


/* SELECT ORDER BY */
SELECT * FROM FLUGLINIE ORDER BY LAND ASC;


/* SELECT ORDER BY WHERE */
SELECT PNR, NAME, LAND FROM PASSAGIER WHERE PNR <= 16;
SELECT PNR, NAME, LAND FROM PASSAGIER WHERE PNR <= 16;

/* SELECT ORDER BY NOTHING */
SELECT * FROM BUCHUNG WHERE BNR > 333;


/* SELECT ORDER BY GROUP BY */
SELECT FLC, COUNT(*) FROM BUCHUNG GROUP BY FLC;


/* SELECT GROUP BY WHERE NOTHING */
SELECT FLC, COUNT(*) FROM FLUG WHERE (VON = 'ZZZ') GROUP BY FLC;


/* SELECT GROUP BY HAVING */
SELECT PNR, SUM(MEILEN) FROM BUCHUNG GROUP BY PNR HAVING COUNT (*) = 1;


/* SELECT GROUP BY HAVING WHERE */
SELECT VON, SUM(PREIS) FROM BUCHUNG WHERE (BNR > 20) GROUP BY VON HAVING COUNT (*) > 2;

rollback;
  