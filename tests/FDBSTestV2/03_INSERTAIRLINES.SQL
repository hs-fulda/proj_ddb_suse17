set echo on;

alter session set nls_language = english;
alter session set nls_date_format = 'DD-MON-YYYY';
alter session set nls_date_language = english;

-- FLUGLINIE has no partition.
-- 2.2.6.4.	If no partitioning clause is given, the entire table hast to be stored on the “first” of your CDBSes.
-- Total: 11 records, all must be stored ONLY in pinatubo
INSERT INTO FLUGLINIE VALUES ('AB', 'D  ', null, 'Air Berlin', null);                                   
INSERT INTO FLUGLINIE VALUES ('AC', 'CDN', null, 'Air Canada', 'Star');                               
INSERT INTO FLUGLINIE VALUES ('AF', 'F  ', null, 'Air France', 'SkyTeam');                            
INSERT INTO FLUGLINIE VALUES ('BA', 'GB ', null, 'British Airways', 'OneWorld');                      
INSERT INTO FLUGLINIE VALUES ('DB', 'D  ', null, 'Database Airlines', null);                            
INSERT INTO FLUGLINIE VALUES ('DI', 'D  ', null, 'Deutsche BA', null);                                  
INSERT INTO FLUGLINIE VALUES ('DL', 'USA', null, 'Delta Airlines', 'SkyTeam');                        
INSERT INTO FLUGLINIE VALUES ('JL', 'J  ', null, 'Japan Airlines', 'OneWorld');                       
INSERT INTO FLUGLINIE VALUES ('LH', 'D  ', null, 'Lufthansa', 'Star');                                
INSERT INTO FLUGLINIE VALUES ('NH', 'J  ', null, 'All Nippon Airways', 'Star');                       
INSERT INTO FLUGLINIE VALUES ('UA', 'USA', null, 'United Airlines', 'Star');                         

update FLUGLINIE set ALLIANZ = 'SkyTeam' where FLC = 'DL';
update FLUGLINIE set ALLIANZ = 'SkyTeam' where FLC = 'AF';
update FLUGLINIE set ALLIANZ = 'OneWorld' where FLC = 'JL';
update FLUGLINIE set ALLIANZ = 'OneWorld' where FLC = 'BA';

update FLUGLINIE set HUB = NULL;

commit;
  