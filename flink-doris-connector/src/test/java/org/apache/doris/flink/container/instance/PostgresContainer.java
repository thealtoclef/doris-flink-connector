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

package org.apache.doris.flink.container.instance;

import org.apache.doris.flink.exception.DorisRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.stream.Stream;

public class PostgresContainer implements ContainerService {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresContainer.class);
    private static final String POSTGRES_VERSION = "postgres:13";
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "123456";
    private static final String DATABASE = "postgres";
    private final org.testcontainers.containers.PostgreSQLContainer<?> postgresContainer;

    public PostgresContainer() {
        postgresContainer = createContainer();
    }

    private org.testcontainers.containers.PostgreSQLContainer<?> createContainer() {
        LOG.info("Will create postgres container.");
        return new org.testcontainers.containers.PostgreSQLContainer<>(POSTGRES_VERSION)
                .withUsername(USERNAME)
                .withPassword(PASSWORD)
                .withDatabaseName(DATABASE)
                .withCommand(
                        "postgres",
                        "-c",
                        "wal_level=logical",
                        "-c",
                        "max_wal_senders=10",
                        "-c",
                        "max_replication_slots=10");
    }

    @Override
    public void startContainer() {
        LOG.info("Starting PostgreSQL container.");
        Startables.deepStart(Stream.of(postgresContainer)).join();
        LOG.info("PostgreSQL container is started.");
    }

    @Override
    public void close() {
        LOG.info("Stopping PostgreSQL container.");
        if (postgresContainer != null) {
            postgresContainer.stop();
        }
        LOG.info("PostgreSQL container is stopped.");
    }

    @Override
    public boolean isRunning() {
        return postgresContainer.isRunning();
    }

    @Override
    public String getInstanceHost() {
        return postgresContainer.getHost();
    }

    @Override
    public Integer getMappedPort(int originalPort) {
        return postgresContainer.getMappedPort(originalPort);
    }

    @Override
    public String getUsername() {
        return USERNAME;
    }

    @Override
    public String getPassword() {
        return PASSWORD;
    }

    @Override
    public String getJdbcUrl() {
        return String.format(
                "jdbc:postgresql://%s:%d/%s", getInstanceHost(), getMappedPort(5432), DATABASE);
    }

    @Override
    public Connection getQueryConnection() {
        try {
            Class.forName("org.postgresql.Driver");
            return DriverManager.getConnection(getJdbcUrl(), USERNAME, PASSWORD);
        } catch (ClassNotFoundException | SQLException e) {
            throw new DorisRuntimeException(e);
        }
    }
}
