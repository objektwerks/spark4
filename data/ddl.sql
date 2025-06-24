drop table persons if exists;
drop table avg_age_by_role if exists;
create table persons (id int not null, age int not null, name varchar(64) not null, role varchar(64) not null);
insert into persons values (1, 24, 'fred', 'husband');
insert into persons values (2, 23, 'wilma', 'wife');
insert into persons values (3, 22, 'barney', 'husband');
insert into persons values (4, 21, 'betty', 'wife');
create table avg_age_by_role (role varchar(64) not null, avgAge double not null);