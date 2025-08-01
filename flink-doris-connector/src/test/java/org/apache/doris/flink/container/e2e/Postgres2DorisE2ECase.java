// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.flink.container.e2e;

import org.apache.doris.flink.container.AbstractE2EService;
import org.apache.doris.flink.container.ContainerUtils;
import org.apache.doris.flink.container.instance.ContainerService;
import org.apache.doris.flink.container.instance.PostgresContainer;
import org.apache.doris.flink.tools.cdc.DatabaseSyncConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Postgres2DorisE2ECase extends AbstractE2EService {
    private static final Logger LOG = LoggerFactory.getLogger(Postgres2DorisE2ECase.class);
    private static final String DATABASE = "test_e2e_postgres";
    private static final String DROP_DATABASE = "DROP DATABASE IF EXISTS " + DATABASE;
    private static final String POSTGRES_CONF = "--" + DatabaseSyncConfig.POSTGRES_CONF;
    private static ContainerService postgresContainerService;

    @BeforeClass
    public static void initPostgresContainer() {
        LOG.info("Trying to Start PostgreSQL container for E2E tests.");
        if (Objects.nonNull(postgresContainerService) && postgresContainerService.isRunning()) {
            LOG.info("The PostgreSQL container has been started and is running status.");
            return;
        }
        postgresContainerService = new PostgresContainer();
        postgresContainerService.startContainer();
        LOG.info("PostgreSQL container was started.");
    }

    @Before
    public void setUp() throws InterruptedException {
        LOG.info("Postgres2DorisE2ECase attempting to acquire semaphore.");
        SEMAPHORE.acquire();
        LOG.info("Postgres2DorisE2ECase semaphore acquired.");
    }

    private List<String> setPostgres2DorisDefaultConfig(List<String> argList) {
        // set default postgres config
        argList.add(POSTGRES_CONF);
        argList.add(HOSTNAME + "=" + getPostgresInstanceHost());
        argList.add(POSTGRES_CONF);
        argList.add(PORT + "=" + getPostgresQueryPort());
        argList.add(POSTGRES_CONF);
        argList.add(USERNAME + "=" + getPostgresUsername());
        argList.add(POSTGRES_CONF);
        argList.add(PASSWORD + "=" + getPostgresPassword());
        argList.add(POSTGRES_CONF);
        argList.add("database-name=postgres");
        argList.add(POSTGRES_CONF);
        argList.add("schema-name=" + DATABASE);
        argList.add(POSTGRES_CONF);
        argList.add("slot.name=test_slot");
        argList.add(POSTGRES_CONF);
        argList.add("decoding.plugin.name=pgoutput");

        setSinkConfDefaultConfig(argList);
        return argList;
    }

    private void startPostgres2DorisJob(String jobName, String resourcePath) {
        LOG.info(
                "start a postgres to doris job. jobName={}, resourcePath={}",
                jobName,
                resourcePath);
        List<String> argList = ContainerUtils.parseFileArgs(resourcePath);
        String[] args = setPostgres2DorisDefaultConfig(argList).toArray(new String[0]);
        submitE2EJob(jobName, args);
    }

    private void initPostgresEnvironment(String sourcePath) {
        LOG.info("Initializing PostgreSQL environment.");
        ContainerUtils.executeSQLStatement(
                getPostgresQueryConnection(), LOG, ContainerUtils.parseFileContentSQL(sourcePath));
    }

    private void initDorisEnvironment() {
        LOG.info("Initializing Doris environment.");
        ContainerUtils.executeSQLStatement(getDorisQueryConnection(), LOG, DROP_DATABASE);
    }

    private void initEnvironment(String jobName, String postgresSourcePath) {
        LOG.info(
                "start to init postgres to doris environment. jobName={}, postgresSourcePath={}",
                jobName,
                postgresSourcePath);
        initPostgresEnvironment(postgresSourcePath);
        initDorisEnvironment();
    }

    protected String getPostgresInstanceHost() {
        return postgresContainerService.getInstanceHost();
    }

    protected Integer getPostgresQueryPort() {
        return postgresContainerService.getMappedPort(5432);
    }

    protected String getPostgresUsername() {
        return postgresContainerService.getUsername();
    }

    protected String getPostgresPassword() {
        return postgresContainerService.getPassword();
    }

    protected Connection getPostgresQueryConnection() {
        return postgresContainerService.getQueryConnection();
    }

    @Test
    public void testPostgres2Doris() throws Exception {
        String jobName = "testPostgres2Doris";
        String resourcePath = "container/e2e/postgres2doris/testPostgres2Doris.txt";
        initEnvironment(jobName, "container/e2e/postgres2doris/testPostgres2Doris_init.sql");
        startPostgres2DorisJob(jobName, resourcePath);

        // wait 2 times checkpoint
        Thread.sleep(20000);
        LOG.info("Start to verify create table result.");
        String tblQuery =
                String.format(
                        "SELECT TABLE_NAME "
                                + "FROM INFORMATION_SCHEMA.TABLES "
                                + "WHERE TABLE_SCHEMA = '%s'",
                        DATABASE);
        List<String> expectedTables =
                Arrays.asList("ods_tbl1_incr", "ods_tbl2_incr", "ods_tbl3_incr", "ods_tbl5_incr");
        ContainerUtils.checkResult(
                getDorisQueryConnection(), LOG, expectedTables, tblQuery, 1, false);

        LOG.info("Start to verify init result.");
        List<String> expected = Arrays.asList("doris_1,1", "doris_2,2", "doris_3,3", "doris_5,5");
        String sql1 =
                "select * from ( select * from test_e2e_postgres.ods_tbl1_incr union all select * from test_e2e_postgres.ods_tbl2_incr union all select * from test_e2e_postgres.ods_tbl3_incr union all select * from test_e2e_postgres.ods_tbl5_incr) res order by 1";
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, expected, sql1, 2);

        // add incremental data
        ContainerUtils.executeSQLStatement(
                getPostgresQueryConnection(),
                LOG,
                "insert into test_e2e_postgres.tbl1 values ('doris_1_1',10)",
                "insert into test_e2e_postgres.tbl2 values ('doris_2_1',11)",
                "insert into test_e2e_postgres.tbl3 values ('doris_3_1',12)",
                "update test_e2e_postgres.tbl1 set age=18 where name='doris_1'",
                "delete from test_e2e_postgres.tbl2 where name='doris_2'");
        Thread.sleep(20000);

        LOG.info("Start to verify incremental data result.");
        List<String> expected2 =
                Arrays.asList(
                        "doris_1,18", "doris_1_1,10", "doris_2_1,11", "doris_3,3", "doris_3_1,12");
        String sql2 =
                "select * from ( select * from test_e2e_postgres.ods_tbl1_incr union all select * from test_e2e_postgres.ods_tbl2_incr union all select * from test_e2e_postgres.ods_tbl3_incr ) res order by 1";
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, expected2, sql2, 2);

        // mock schema change - PostgreSQL specific DDL commands
        LOG.info("start to schema change in postgres.");
        ContainerUtils.executeSQLStatement(
                getPostgresQueryConnection(),
                LOG,
                "alter table test_e2e_postgres.tbl1 add column c1 varchar(128)",
                "alter table test_e2e_postgres.tbl1 drop column age",
                "alter table test_e2e_postgres.tbl2 rename column age to age_new_1",
                "alter table test_e2e_postgres.tbl3 rename column age to age_new_2");
        Thread.sleep(10000);
        ContainerUtils.executeSQLStatement(
                getPostgresQueryConnection(),
                LOG,
                "insert into test_e2e_postgres.tbl1 values ('doris_1_1_1','c1_val')",
                "insert into test_e2e_postgres.tbl2 values ('doris_tbl2_rename_test',18)",
                "insert into test_e2e_postgres.tbl3 values ('doris_tbl3_rename_test',38)");
        Thread.sleep(20000);
        LOG.info("verify tbl1 schema change.");
        List<String> schemaChangeExpected =
                Arrays.asList("doris_1,null", "doris_1_1,null", "doris_1_1_1,c1_val");
        String schemaChangeSql = "select name,c1 from test_e2e_postgres.ods_tbl1_incr order by 1";
        ContainerUtils.checkResult(
                getDorisQueryConnection(), LOG, schemaChangeExpected, schemaChangeSql, 2);

        LOG.info(
                "verify tbl2 schema change (additive-only: old records have null in renamed column).");
        schemaChangeExpected = Arrays.asList("doris_2_1,null", "doris_tbl2_rename_test,18");
        schemaChangeSql = "select name,age_new_1 from test_e2e_postgres.ods_tbl2_incr order by 1";
        ContainerUtils.checkResult(
                getDorisQueryConnection(), LOG, schemaChangeExpected, schemaChangeSql, 2);

        LOG.info(
                "verify tbl3 schema change (additive-only: old records have null in renamed column).");
        schemaChangeExpected =
                Arrays.asList("doris_3,null", "doris_3_1,null", "doris_tbl3_rename_test,38");
        schemaChangeSql = "select name,age_new_2 from test_e2e_postgres.ods_tbl3_incr order by 1";
        ContainerUtils.checkResult(
                getDorisQueryConnection(), LOG, schemaChangeExpected, schemaChangeSql, 2);

        cancelE2EJob(jobName);
    }

    @Test
    public void testAutoAddTable() throws Exception {
        String jobName = "testAutoAddTable";
        String resourcePath = "container/e2e/postgres2doris/testPostgres2Doris.txt";
        initEnvironment(jobName, "container/e2e/postgres2doris/testPostgres2Doris_init.sql");
        startPostgres2DorisJob(jobName, resourcePath);

        // wait 2 times checkpoint
        Thread.sleep(20000);
        LOG.info("Start to verify init result.");
        List<String> expected = Arrays.asList("doris_1,1", "doris_2,2", "doris_3,3", "doris_5,5");
        String sql1 =
                "select * from ( select * from test_e2e_postgres.ods_tbl1_incr union all select * from test_e2e_postgres.ods_tbl2_incr union all select * from test_e2e_postgres.ods_tbl3_incr union all select * from test_e2e_postgres.ods_tbl5_incr) res order by 1";
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, expected, sql1, 2);

        // Test auto table discovery: create new table that matches pattern after job starts
        LOG.info("Testing auto table discovery - creating new table tbl6");
        ContainerUtils.executeSQLStatement(
                getPostgresQueryConnection(),
                LOG,
                "CREATE TABLE test_e2e_postgres.tbl6 (name varchar(256) primary key, age int)",
                "INSERT INTO test_e2e_postgres.tbl6 VALUES ('auto_discovered',99)");
        Thread.sleep(15000);

        // Verify the new table was auto-discovered and data was synced
        LOG.info("Verify auto table discovery result");
        List<String> autoDiscoveryExpected = Arrays.asList("ods_tbl6_incr");
        String autoDiscoveryTableQuery =
                String.format(
                        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = 'ods_tbl6_incr'",
                        DATABASE);
        ContainerUtils.checkResult(
                getDorisQueryConnection(),
                LOG,
                autoDiscoveryExpected,
                autoDiscoveryTableQuery,
                1,
                false);

        // Verify data in auto-discovered table
        List<String> autoDiscoveryDataExpected = Arrays.asList("auto_discovered,99");
        String autoDiscoveryDataSql =
                "select name,age from test_e2e_postgres.ods_tbl6_incr order by 1";
        ContainerUtils.checkResult(
                getDorisQueryConnection(), LOG, autoDiscoveryDataExpected, autoDiscoveryDataSql, 2);

        // Verify that primary key was correctly transferred from PostgreSQL to Doris
        LOG.info("Verify primary key constraint was transferred to auto-discovered table");

        // Get primary key from PostgreSQL source table
        String postgresPkSql =
                "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = 'test_e2e_postgres.tbl6'::regclass AND i.indisprimary ORDER BY a.attnum";
        List<String> postgresPkColumns =
                ContainerUtils.getResult(
                        getPostgresQueryConnection(), LOG, Arrays.asList("name"), postgresPkSql, 1);
        LOG.info("PostgreSQL source table primary key columns: {}", postgresPkColumns);

        // Get primary key from Doris target table using SHOW CREATE TABLE
        List<String> dorisPkColumns = getDorisTableKey(DATABASE, "ods_tbl6_incr", "UNIQUE KEY");
        LOG.info("Doris target table primary key columns: {}", dorisPkColumns);

        // Also verify that the table has UNIQUE KEY (not DUPLICATE KEY) since PostgreSQL table has
        // primary key
        String createTableSql = getCreateTableSQL(DATABASE, "ods_tbl6_incr");
        Assert.assertTrue(
                "Auto-discovered table should have UNIQUE KEY since source has primary key",
                createTableSql.contains("UNIQUE KEY"));
        Assert.assertFalse(
                "Auto-discovered table should not have DUPLICATE KEY since source has primary key",
                createTableSql.contains("DUPLICATE KEY"));

        // Verify the primary keys match between source and target
        Assert.assertEquals(
                "Primary key columns should match between PostgreSQL source and Doris target",
                postgresPkColumns,
                dorisPkColumns);

        // Also verify we have the expected primary key
        List<String> expectedPrimaryKey = Arrays.asList("name");
        Assert.assertEquals(
                "Auto-discovered table should have 'name' as primary key",
                expectedPrimaryKey,
                dorisPkColumns);

        // incremental data
        LOG.info("starting to increment data.");
        ContainerUtils.executeSQLStatement(
                getPostgresQueryConnection(),
                LOG,
                "insert into test_e2e_postgres.tbl1 values ('doris_1_1',10)",
                "insert into test_e2e_postgres.tbl2 values ('doris_2_1',11)",
                "insert into test_e2e_postgres.tbl3 values ('doris_3_1',12)",
                "update test_e2e_postgres.tbl1 set age=18 where name='doris_1'",
                "delete from test_e2e_postgres.tbl2 where name='doris_2'",
                "insert into test_e2e_postgres.tbl6 values ('auto_add_incr',100)",
                "delete from test_e2e_postgres.tbl6 where name='auto_discovered'",
                "update test_e2e_postgres.tbl6 set age=101 where name='auto_add_incr'");
        Thread.sleep(20000);

        List<String> incrementDataExpected =
                Arrays.asList(
                        "auto_add_incr,101",
                        "doris_1,18",
                        "doris_1_1,10",
                        "doris_2_1,11",
                        "doris_3,3",
                        "doris_3_1,12");
        String incrementDataSql =
                "select * from ( select name, age from test_e2e_postgres.ods_tbl1_incr union all select name, age from test_e2e_postgres.ods_tbl2_incr union all select name, age from test_e2e_postgres.ods_tbl3_incr union all select name, age from test_e2e_postgres.ods_tbl6_incr) res order by 1";
        ContainerUtils.checkResult(
                getDorisQueryConnection(), LOG, incrementDataExpected, incrementDataSql, 2);

        // schema change on auto-added table
        LOG.info("starting to mock schema change on auto-added table.");
        ContainerUtils.executeSQLStatement(
                getPostgresQueryConnection(),
                LOG,
                "alter table test_e2e_postgres.tbl6 add column c1 varchar(128)",
                "alter table test_e2e_postgres.tbl6 drop column age",
                "insert into test_e2e_postgres.tbl6 values ('auto_schema_change','c1_val')");
        Thread.sleep(20000);
        List<String> schemaChangeExpected =
                Arrays.asList("auto_add_incr,null", "auto_schema_change,c1_val");
        String schemaChangeSql = "select name,c1 from test_e2e_postgres.ods_tbl6_incr order by 1";
        ContainerUtils.checkResult(
                getDorisQueryConnection(), LOG, schemaChangeExpected, schemaChangeSql, 2);

        cancelE2EJob(jobName);
    }

    private List<String> getDorisTableKey(String database, String table, String keyType)
            throws Exception {
        Statement statement = getDorisQueryConnection().createStatement();
        ResultSet resultSet =
                statement.executeQuery(String.format("SHOW CREATE TABLE %s.%s", database, table));
        List<String> primaryKeys = new ArrayList<>();

        while (resultSet.next()) {
            String createTblSql = resultSet.getString(2);
            LOG.info(
                    "Create table sql for {}.{}: {}",
                    database,
                    table,
                    createTblSql.replace("\n", " "));

            // Parse the specified key type from DDL
            // Look for patterns like: UNIQUE KEY(`name`) or DUPLICATE KEY(`id`, `name`)
            String[] lines = createTblSql.split("\n");
            for (String line : lines) {
                line = line.trim();
                if (line.contains(keyType)) {
                    // Extract column names from patterns like "UNIQUE KEY(`name`)" or "DUPLICATE
                    // KEY(`id`, `name`)"
                    int startIdx = line.indexOf('(');
                    int endIdx = line.indexOf(')', startIdx);
                    if (startIdx != -1 && endIdx != -1) {
                        String keyColumns = line.substring(startIdx + 1, endIdx);
                        // Remove backticks and split by comma
                        String[] columns = keyColumns.replace("`", "").split(",");
                        for (String col : columns) {
                            primaryKeys.add(col.trim());
                        }
                        break; // Take the first key found
                    }
                }
            }
            break;
        }
        return primaryKeys;
    }

    private String getCreateTableSQL(String database, String table) throws Exception {
        Statement statement = getDorisQueryConnection().createStatement();
        ResultSet resultSet =
                statement.executeQuery(String.format("SHOW CREATE TABLE %s.%s", database, table));
        while (resultSet.next()) {
            String createTblSql = resultSet.getString(2);
            LOG.info("Create table sql: {}", createTblSql.replace("\n", ""));
            return createTblSql;
        }
        throw new RuntimeException("Table not exist " + table);
    }

    @Test
    public void testPostgres2DorisNoPK() throws Exception {
        String jobName = "testPostgres2DorisNoPK";
        initEnvironment(jobName, "container/e2e/postgres2doris/testPostgres2DorisNoPK_init.sql");
        startPostgres2DorisJob(jobName, "container/e2e/postgres2doris/testPostgres2DorisNoPK.txt");

        // wait 2 times checkpoint
        Thread.sleep(20000);
        LOG.info("Start to verify table creation and data for tables without primary keys.");

        // Check that tables were created
        String tblQuery =
                String.format(
                        "SELECT TABLE_NAME \n"
                                + "FROM INFORMATION_SCHEMA.TABLES \n"
                                + "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME LIKE 'ods%%nopk%%'",
                        DATABASE);
        List<String> expectedTables =
                Arrays.asList("ods_tbl_nopk1_nopk", "ods_tbl_nopk2_nopk", "ods_tbl_nopk3_nopk");
        ContainerUtils.checkResult(
                getDorisQueryConnection(), LOG, expectedTables, tblQuery, 1, true);

        LOG.info("Start to verify init data for tables without primary keys.");
        List<String> expected1 = Arrays.asList("1,doris_1,18");
        String sql1 = "select id, name, age from test_e2e_postgres.ods_tbl_nopk1_nopk order by id";
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, expected1, sql1, 3);

        List<String> expected2 = Arrays.asList("1,doris_2,25,Engineering");
        String sql2 =
                "select id, name, age, department from test_e2e_postgres.ods_tbl_nopk2_nopk order by id";
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, expected2, sql2, 4);

        List<String> expected3 = Arrays.asList("1,doris_3,doris3@example.com,30");
        String sql3 =
                "select id, name, email, age from test_e2e_postgres.ods_tbl_nopk3_nopk order by id";
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, expected3, sql3, 4);

        // Verify table schema - all tables should be DUPLICATE key model since no primary keys
        String createTblSQL1 = getCreateTableSQL(DATABASE, "ods_tbl_nopk1_nopk");
        Assert.assertTrue(
                "Table without PK should use DUPLICATE key model",
                createTblSQL1.contains("DUPLICATE KEY"));
        Assert.assertFalse(
                "Table without PK should not use UNIQUE key model",
                createTblSQL1.contains("UNIQUE KEY"));

        String createTblSQL2 = getCreateTableSQL(DATABASE, "ods_tbl_nopk2_nopk");
        Assert.assertTrue(
                "Table without PK should use DUPLICATE key model",
                createTblSQL2.contains("DUPLICATE KEY"));
        Assert.assertFalse(
                "Table without PK should not use UNIQUE key model",
                createTblSQL2.contains("UNIQUE KEY"));

        String createTblSQL3 = getCreateTableSQL(DATABASE, "ods_tbl_nopk3_nopk");
        Assert.assertTrue(
                "Table without PK should use DUPLICATE key model",
                createTblSQL3.contains("DUPLICATE KEY"));
        Assert.assertFalse(
                "Table without PK should not use UNIQUE key model",
                createTblSQL3.contains("UNIQUE KEY"));

        // Test incremental data operations on tables without primary keys
        LOG.info("Start to add incremental data for tables without primary keys.");
        ContainerUtils.executeSQLStatement(
                getPostgresQueryConnection(),
                LOG,
                "INSERT INTO test_e2e_postgres.tbl_nopk1 VALUES (2, 'doris_1_new', 22, '2023-01-02 10:00:00')",
                "INSERT INTO test_e2e_postgres.tbl_nopk1 VALUES (1, 'doris_1_dup', 19, '2023-01-03 10:00:00')", // duplicate id is allowed without PK
                "INSERT INTO test_e2e_postgres.tbl_nopk2 VALUES (2, 'doris_2_new', 28, 'Marketing')",
                "INSERT INTO test_e2e_postgres.tbl_nopk3 VALUES (2, 'doris_3_new', 'doris3new@example.com', 32)");

        Thread.sleep(20000);

        LOG.info("Start to verify incremental data for tables without primary keys.");
        // Note: DUPLICATE key model allows duplicate records, so we should see all records
        List<String> incrementalExpected1 =
                Arrays.asList("1,doris_1,18", "1,doris_1_dup,19", "2,doris_1_new,22");
        String incrementalSql1 =
                "select id, name, age from test_e2e_postgres.ods_tbl_nopk1_nopk order by id, name";
        ContainerUtils.checkResult(
                getDorisQueryConnection(), LOG, incrementalExpected1, incrementalSql1, 3);

        List<String> incrementalExpected2 =
                Arrays.asList("1,doris_2,25,Engineering", "2,doris_2_new,28,Marketing");
        String incrementalSql2 =
                "select id, name, age, department from test_e2e_postgres.ods_tbl_nopk2_nopk order by id";
        ContainerUtils.checkResult(
                getDorisQueryConnection(), LOG, incrementalExpected2, incrementalSql2, 4);

        List<String> incrementalExpected3 =
                Arrays.asList(
                        "1,doris_3,doris3@example.com,30",
                        "2,doris_3_new,doris3new@example.com,32");
        String incrementalSql3 =
                "select id, name, email, age from test_e2e_postgres.ods_tbl_nopk3_nopk order by id";
        ContainerUtils.checkResult(
                getDorisQueryConnection(), LOG, incrementalExpected3, incrementalSql3, 4);

        cancelE2EJob(jobName);
    }

    @After
    public void close() {
        try {
            // Ensure that semaphore is always released
        } finally {
            LOG.info("Postgres2DorisE2ECase releasing semaphore.");
            SEMAPHORE.release();
        }
    }
}
