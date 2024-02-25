create database test_db;
go

create login test_user with password = 'test_pass';
go

use test_db;
go

create user test_user for login test_user;
go

use test_db;
go

create schema test_pgps;
go

create table test_pgps.table1(
  id integer primary key,
  text1 text
);
  
insert into test_pgps.table1 values (1,'one'), (2,'two');
  
grant select, update, insert, delete on test_pgps.table1 to test_user;
