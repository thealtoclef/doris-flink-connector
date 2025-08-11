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

package org.apache.doris.flink.tools.cdc.postgres.serializer;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.doris.flink.catalog.doris.DataModel;
import org.apache.doris.flink.catalog.doris.DorisSystem;
import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumChangeContext;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumSchemaChange;
import org.apache.doris.flink.tools.cdc.SourceSchema;
import org.apache.doris.flink.tools.cdc.postgres.PostgresSchema;
import org.apache.doris.flink.tools.cdc.postgres.PostgresType;
import org.apache.doris.flink.tools.cdc.utils.DorisTableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumChangeUtils.getDorisTableIdentifier;

public class PostgresJsonDebeziumSchemaChange extends JsonDebeziumSchemaChange {
    private static final Logger LOG =
            LoggerFactory.getLogger(PostgresJsonDebeziumSchemaChange.class);

    private final Map<String, Map<String, String>> tableFields;
    private final DorisSystem dorisSystem;
    private final String jdbcUrl;
    private final Properties connectionProps;

    public PostgresJsonDebeziumSchemaChange(
            JsonDebeziumChangeContext changeContext, String jdbcUrl, Properties connectionProps) {
        super(changeContext);
        this.jdbcUrl = jdbcUrl;
        this.connectionProps = connectionProps;
        this.dorisSystem = new DorisSystem(dorisOptions);
        this.tableFields = new HashMap<>();
    }

    @Override
    public boolean schemaChange(JsonNode recordRoot) {
        // Parse table identifiers
        String cdcTableIdentifier = getCdcTableIdentifier(recordRoot);
        String dorisTableIdentifier =
                getDorisTableIdentifier(cdcTableIdentifier, dorisOptions, tableMapping);
        String[] split = dorisTableIdentifier.split("\\.");
        String dorisDatabase = split[0];
        String dorisTable = split[1];

        // Build table field mapping if not exists
        String identifier = dorisDatabase + "." + dorisTable;
        tableFields.computeIfAbsent(
                identifier, k -> dorisSystem.getTableFieldNames(dorisDatabase, dorisTable));

        // Process schema changes from 'after' data
        JsonNode afterNode = recordRoot.get("after");
        if (afterNode != null && !afterNode.isNull()) {
            Map<String, String> tableFieldMap = tableFields.get(dorisTableIdentifier);
            if (tableFieldMap == null) {
                LOG.warn("No field mapping found for table {}", dorisTableIdentifier);
                return true;
            }

            // Find new fields that don't exist in current table schema
            Set<String> newFields = new HashSet<>();
            afterNode
                    .fieldNames()
                    .forEachRemaining(
                            fieldName -> {
                                if (!tableFieldMap.containsKey(fieldName)) {
                                    newFields.add(fieldName);
                                }
                            });

            // Add new columns to table schema
            for (String newField : newFields) {
                try {
                    JsonNode fieldValue = afterNode.get(newField);
                    String dorisType = PostgresType.jsonNodeToDorisType(fieldValue);

                    LOG.info(
                            "Adding column {} with type {} to table {}.{}",
                            newField,
                            dorisType,
                            dorisDatabase,
                            dorisTable);

                    FieldSchema fieldSchema = new FieldSchema(newField, dorisType, null);
                    schemaChangeManager.addColumn(dorisDatabase, dorisTable, fieldSchema);

                    // Update local field mapping cache
                    tableFields.get(dorisTableIdentifier).put(newField, dorisType);

                    LOG.info(
                            "Added new column {} to table {}.{}",
                            newField,
                            dorisDatabase,
                            dorisTable);
                } catch (Exception e) {
                    if (e.getMessage() != null && e.getMessage().contains("Nothing is changed")) {
                        LOG.debug(
                                "Column {} already exists in table {}.{}, skipping",
                                newField,
                                dorisDatabase,
                                dorisTable);
                    } else {
                        String errorMsg =
                                String.format(
                                        "Failed to add column %s to table %s.%s: %s",
                                        newField, dorisDatabase, dorisTable, e.getMessage());
                        LOG.error(errorMsg, e);
                        throw new DorisRuntimeException(errorMsg, e);
                    }
                }
            }
        }

        return true;
    }

    public String createDorisTable(JsonNode recordRoot) {
        // Parse table identifiers
        String cdcTableIdentifier = getCdcTableIdentifier(recordRoot);

        try {
            String[] split = cdcTableIdentifier.split("\\.");
            String sourceSchema = split[1];
            String sourceTable = split[2];
            String dorisTable = tableNameConverter.convert(sourceTable);
            String dorisTableName = String.format("%s.%s", this.targetDatabase, dorisTable);

            // Fetch the schema from the source database and create the table in Doris
            LOG.info("Creating Doris table {}", dorisTableName);
            SourceSchema postgresSchema = fetchPostgresSchema(sourceSchema, sourceTable);
            DorisTableUtil.tryCreateTableIfAbsent(
                    dorisSystem,
                    this.targetDatabase,
                    dorisTable,
                    postgresSchema,
                    this.dorisTableConfig);

            // Add the table to the table mapping
            tableMapping.put(cdcTableIdentifier, dorisTableName);
            return dorisTableName;
        } catch (Exception e) {
            throw new DorisRuntimeException(
                    String.format(
                            "Failed to parse CDC table identifier '%s'. Record source: %s, record op: %s",
                            cdcTableIdentifier, recordRoot.get("source"), recordRoot.get("op")),
                    e);
        }
    }

    private SourceSchema fetchPostgresSchema(String sourceSchemaName, String sourceTable) {
        if (jdbcUrl == null || connectionProps == null) {
            String errorMsg = "No source connection configuration available for schema fetching";
            LOG.error(errorMsg);
            throw new DorisRuntimeException(errorMsg);
        }

        try (Connection connection = DriverManager.getConnection(jdbcUrl, connectionProps)) {
            DatabaseMetaData metaData = connection.getMetaData();

            // Extract database name from PostgreSQL JDBC URL
            String sourceDatabaseName;
            if (jdbcUrl != null && jdbcUrl.startsWith("jdbc:postgresql://")) {
                String[] parts = jdbcUrl.split("/");
                if (parts.length >= 4) {
                    String dbPart = parts[3];
                    int queryIndex = dbPart.indexOf('?');
                    sourceDatabaseName = queryIndex > 0 ? dbPart.substring(0, queryIndex) : dbPart;
                } else {
                    throw new DorisRuntimeException(
                            "Unable to extract database name from JDBC URL: " + jdbcUrl);
                }
            } else {
                throw new DorisRuntimeException("Invalid PostgreSQL JDBC URL: " + jdbcUrl);
            }

            PostgresSchema schema =
                    new PostgresSchema(
                            metaData, sourceDatabaseName, sourceSchemaName, sourceTable, "");
            schema.setModel(
                    !schema.getPrimaryKeys().isEmpty() ? DataModel.UNIQUE : DataModel.DUPLICATE);

            // Add CDC timestamp fields for consistency with snapshot phase
            DorisTableUtil.addCdcTimestampFields(schema);

            LOG.info(
                    "Fetched PostgreSQL schema for {}.{}: primaryKeys={}, fields={}",
                    sourceSchemaName,
                    sourceTable,
                    schema.getPrimaryKeys(),
                    schema.getFields().keySet());
            return schema;
        } catch (SQLException e) {
            String errorMsg =
                    String.format(
                            "Failed to fetch PostgreSQL schema for %s.%s: %s",
                            sourceSchemaName, sourceTable, e.getMessage());
            LOG.error(errorMsg, e);
            throw new DorisRuntimeException(errorMsg, e);
        } catch (Exception e) {
            String errorMsg =
                    String.format(
                            "Failed to create PostgresSchema for %s.%s: %s",
                            sourceSchemaName, sourceTable, e.getMessage());
            LOG.error(errorMsg, e);
            throw new DorisRuntimeException(errorMsg, e);
        }
    }
}
