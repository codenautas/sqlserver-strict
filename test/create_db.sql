create database test_db;
go

create login test_user with password = 'test_pass';
go

GRANT CREATE TABLE ON DATABASE::test_db TO test_user;
go

use test_db;
go

create user test_user for login test_user;
go

create schema test_pgps;
go

GRANT CONTROL ON SCHEMA::test_pgps TO test_user;
go

GRANT CREATE SCHEMA TO test_user;
go