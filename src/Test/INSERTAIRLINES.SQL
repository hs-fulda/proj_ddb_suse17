set echo on;

alter session set nls_language = english;
alter session set nls_date_format = 'DD-MON-YYYY';
alter session set nls_date_language = english;


-- FLC	varchar(2), LAND  varchar(3), HUB  varchar(3), NAME  varchar(30), ALLIANZ varchar(20)
-- has HORIZONTAL (FLC('KK','MM')) applied to 1st column
-- oralv8a: 9 tuples
INSERT INTO FLUGLINIE VALUES ('AB', 'D  ', null, 'Air Berlin', null);                                   
INSERT INTO FLUGLINIE VALUES ('AC', 'CDN', null, 'Air Canada', 'Star');                               
INSERT INTO FLUGLINIE VALUES ('AF', 'F  ', null, 'Air France', 'SkyTeam');
INSERT INTO FLUGLINIE VALUES ('BA', 'GB ', null, 'British Airways', 'OneWorld');     
INSERT INTO FLUGLINIE VALUES ('DB', 'D  ', null, 'Database Airlines', null);                            
INSERT INTO FLUGLINIE VALUES ('DI', 'D  ', null, 'Deutsche BA', null);                                  
INSERT INTO FLUGLINIE VALUES ('DL', 'USA', null, 'Delta Airlines', 'SkyTeam');                      
INSERT INTO FLUGLINIE VALUES ('JL', 'J  ', null, 'Japan Airlines', 'OneWorld');              
-- oralv9a
INSERT INTO FLUGLINIE VALUES ('LH', 'D  ', null, 'Lufthansa', 'Star');
-- oralv10a
INSERT INTO FLUGLINIE VALUES ('NH', 'J  ', null, 'All Nippon Airways', 'Star');                       
INSERT INTO FLUGLINIE VALUES ('UA', 'USA', null, 'United Airlines', 'Star');                         

update FLUGLINIE set ALLIANZ = 'SkyTeam' where FLC = 'DL';
update FLUGLINIE set ALLIANZ = 'SkyTeam' where FLC = 'AF';
update FLUGLINIE set ALLIANZ = 'OneWorld' where FLC = 'JL';
update FLUGLINIE set ALLIANZ = 'OneWorld' where FLC = 'BA';

update FLUGLINIE set HUB = NULL;
-- every UPDATE below will update tuples from lines 20,	13,14,18.
update FLUGLINIE set HUB = 'FRA' where FLC = 'LH';
update FLUGLINIE set HUB = 'CDG' where FLC = 'AF';
update FLUGLINIE set HUB = 'LHR' where FLC = 'BA';
update FLUGLINIE set HUB = 'NRT' where FLC = 'JL';

commit;
  