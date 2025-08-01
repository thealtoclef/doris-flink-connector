DROP SCHEMA IF EXISTS test_e2e_postgres CASCADE;
CREATE SCHEMA test_e2e_postgres;

-- Table without primary key - should result in DUPLICATE key model in Doris
DROP TABLE IF EXISTS test_e2e_postgres.tbl_nopk1;
CREATE TABLE test_e2e_postgres.tbl_nopk1 (
    id integer,
    name varchar(256),
    age integer,
    create_time timestamp DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO test_e2e_postgres.tbl_nopk1 VALUES (1, 'doris_1', 18, '2023-01-01 10:00:00');

-- Another table without primary key
DROP TABLE IF EXISTS test_e2e_postgres.tbl_nopk2;
CREATE TABLE test_e2e_postgres.tbl_nopk2 (
    id integer,
    name varchar(256),
    age integer,
    department varchar(100)
);
INSERT INTO test_e2e_postgres.tbl_nopk2 VALUES (1, 'doris_2', 25, 'Engineering');

-- Table without primary key but with unique index - should still be DUPLICATE key model
DROP TABLE IF EXISTS test_e2e_postgres.tbl_nopk3;
CREATE TABLE test_e2e_postgres.tbl_nopk3 (
    id integer,
    name varchar(256),
    email varchar(256),
    age integer
);
CREATE UNIQUE INDEX idx_email ON test_e2e_postgres.tbl_nopk3 (email);
INSERT INTO test_e2e_postgres.tbl_nopk3 VALUES (1, 'doris_3', 'doris3@example.com', 30);