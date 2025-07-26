DROP SCHEMA IF EXISTS test_e2e_postgres CASCADE;
CREATE SCHEMA test_e2e_postgres;

DROP TABLE IF EXISTS test_e2e_postgres.tbl1;
CREATE TABLE test_e2e_postgres.tbl1 (
    name varchar(256) primary key,
    age int
);
insert into test_e2e_postgres.tbl1 values ('doris_1',1);

DROP TABLE IF EXISTS test_e2e_postgres.tbl2;
CREATE TABLE test_e2e_postgres.tbl2 (
    name varchar(256) primary key,
    age int
);
insert into test_e2e_postgres.tbl2 values ('doris_2',2);

DROP TABLE IF EXISTS test_e2e_postgres.tbl3;
CREATE TABLE test_e2e_postgres.tbl3 (
    name varchar(256) primary key,
    age int
);
insert into test_e2e_postgres.tbl3 values ('doris_3',3);

DROP TABLE IF EXISTS test_e2e_postgres.tbl4;
CREATE TABLE test_e2e_postgres.tbl4 (
    name varchar(256) primary key,
    age int
);
insert into test_e2e_postgres.tbl4 values ('doris_4',4);

DROP TABLE IF EXISTS test_e2e_postgres.tbl5;
CREATE TABLE test_e2e_postgres.tbl5 (
    name varchar(256) primary key,
    age int
);
insert into test_e2e_postgres.tbl5 values ('doris_5',5);