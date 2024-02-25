create database test_db;
go

create login test_user with password = 'test_pass';
go

use test_db;
go

create user test_user for login test_user;
go
