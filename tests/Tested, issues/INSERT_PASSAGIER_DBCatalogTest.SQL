-- RUN the program in Debug mode and observe whether it send each statement only once and directly to the required database
--5.1	Meta data required to describe the horizontal partitioning of global tables over the CDBSes in the federation, need to be designed and implemented. 
--5.2	☐These meta data should be stored in either one or all databases to later enable your statement parser to identify and locate all the partitions to be queried to generate the global result of a SELECT statement. The design/format of the meta data (the catalogue) is to your discretion.
INSERT INTO PASSAGIER VALUES (34, 'test of db catalogue', 'for PINATUBO', 'D');
INSERT INTO PASSAGIER VALUES (54, 'test of db catalogue', 'for MTSHELENS', 'D');
INSERT INTO PASSAGIER VALUES (84, 'test of db catalogue', 'for KRAKATAU', 'D');