DROP DATABASE if EXISTS test_e2e_mysql;
CREATE DATABASE if NOT EXISTS test_e2e_mysql;

-- Table without primary key - should result in DUPLICATE key model in Doris
DROP TABLE IF EXISTS test_e2e_mysql.tbl_nopk1;
CREATE TABLE test_e2e_mysql.tbl_nopk1 (
    `id` int,
    `name` varchar(256),
    `age` int,
    `create_time` timestamp DEFAULT CURRENT_TIMESTAMP
);
insert into test_e2e_mysql.tbl_nopk1 values (1, 'doris_1', 18, '2023-01-01 10:00:00');

-- Another table without primary key
DROP TABLE IF EXISTS test_e2e_mysql.tbl_nopk2;
CREATE TABLE test_e2e_mysql.tbl_nopk2 (
    `id` int,
    `name` varchar(256),
    `age` int,
    `department` varchar(100)
);
insert into test_e2e_mysql.tbl_nopk2 values (1, 'doris_2', 25, 'Engineering');

-- Table without primary key but with unique index - should still be DUPLICATE key model
DROP TABLE IF EXISTS test_e2e_mysql.tbl_nopk3;
CREATE TABLE test_e2e_mysql.tbl_nopk3 (
    `id` int,
    `name` varchar(256),
    `email` varchar(256),
    `age` int,
    UNIQUE KEY `idx_email` (`email`)
);
insert into test_e2e_mysql.tbl_nopk3 values (1, 'doris_3', 'doris3@example.com', 30);