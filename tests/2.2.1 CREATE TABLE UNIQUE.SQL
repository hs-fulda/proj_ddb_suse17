create table test_colors (
color		varchar(10),
constraint color_UQ UNIQUE (color)
);
INSERT INTO test_colors values ('red');
commit;
SELECT * FROM test_colors;
INSERT INTO test_colors values ('red');
Drop table test_colors;