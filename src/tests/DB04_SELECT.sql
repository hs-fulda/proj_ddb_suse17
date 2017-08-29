-- User: Aleks A. Partitioned TABLE tests: 
-- 1. part_tab MUST BE CREATED in oralv8a, oralv9a, oralv10a
create table part_tab (
PartKey	integer,
SID	varchar(9),
constraint SID_NN check (SID is not null),
constraint PartKey primary key (PartKey)
)
HORIZONTAL (PartKey(89,99));
-- 2. MUST BE DISTRIBUTED TO oralv8a:
INSERT INTO part_tab VALUES (88, 'oralv8a');
INSERT INTO part_tab VALUES (89, 'oralv8a');
-- 3. MUST BE DISTRIBUTED TO oralv9a:
INSERT INTO part_tab VALUES (90, 'oralv9a');
INSERT INTO part_tab VALUES (99, 'oralv9a');
-- 4. MUST BE DISTRIBUTED TO oralv10a:
INSERT INTO part_tab VALUES (100, 'oralv10a');
INSERT INTO part_tab VALUES (101, 'oralv10a');
-- 5. MUST SELECT 3*2 = 6 tuples
SELECT * FROM part_tab;
DELETE FROM part_tab;
commit;
-- 6. MUST SELECT 0 tuples:
SELECT * FROM part_tab;
rollback;
-- 7. MUST SELECT 6 tuples
SELECT * FROM part_tab;