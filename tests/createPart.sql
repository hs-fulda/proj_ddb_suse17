create table TJ_PASSAGIER (
PNR		  integer,
NAME		varchar(40),
VORNAME		varchar(40),
LAND		varchar(3),
constraint TJ_PASSAGIER_NAME_NN
                check (NAME is not null),
constraint TJ_PASSAGIER_PS
		primary key (PNR)
)
HORIZONTAL (PNR(35,70));