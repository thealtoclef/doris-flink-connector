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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.catalog.doris.DataModel;
import org.apache.doris.flink.catalog.doris.DorisSystem;
import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.sink.schema.SchemaChangeManager;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.CdcSchemaChange;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumChangeContext;
import org.apache.doris.flink.tools.cdc.SourceSchema;
import org.apache.doris.flink.tools.cdc.postgres.PostgresType;
import org.apache.doris.flink.tools.cdc.utils.DorisTableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumChangeUtils.extractJsonNode;
import static org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumChangeUtils.getDorisTableIdentifier;

public class PostgresJsonDebeziumSchemaChange extends CdcSchemaChange {

    private static final Logger LOG =
            LoggerFactory.getLogger(PostgresJsonDebeziumSchemaChange.class);

    private final ObjectMapper objectMapper;
    private final Map<String, Map<String, String>> tableFields;
    private SchemaChangeManager schemaChangeManager;
    private DorisSystem dorisSystem;
    public Map<String, String> tableMapping;
    private final DorisOptions dorisOptions;
    public JsonDebeziumChangeContext changeContext;

    public PostgresJsonDebeziumSchemaChange(JsonDebeziumChangeContext changeContext) {
        this.changeContext = changeContext;
        this.objectMapper = changeContext.getObjectMapper();
        this.dorisOptions = changeContext.getDorisOptions();
        this.tableFields = new HashMap<>();
        this.schemaChangeManager = new SchemaChangeManager(dorisOptions);
        this.dorisSystem = new DorisSystem(dorisOptions);
        this.tableMapping = changeContext.getTableMapping();
    }

    @Override
    public String extractDatabase(JsonNode record) {
        return null;
    }

    @Override
    public String extractTable(JsonNode record) {
        return null;
    }

    @Override
    public boolean schemaChange(JsonNode recordRoot) {
        try {
            String cdcTableIdentifier = getCdcTableIdentifier(recordRoot);
            String dorisTableIdentifier =
                    getDorisTableIdentifier(cdcTableIdentifier, dorisOptions, tableMapping);

            // if table dorisTableIdentifier is null, create table (auto-discovery)
            if (StringUtils.isNullOrWhitespaceOnly(dorisTableIdentifier)) {
                String[] split = cdcTableIdentifier.split("\\.");
                if (split.length != 2) {
                    LOG.error("Invalid CDC table identifier: {}", cdcTableIdentifier);
                    return false;
                }
                String targetDb = changeContext.getTargetDatabase();
                String sourceTable = split[1];
                String dorisTable = changeContext.getTableNameConverter().convert(sourceTable);
                LOG.info(
                        "The table [{}.{}] does not exist. Attempting to create a new table named: {}.{}",
                        targetDb,
                        sourceTable,
                        targetDb,
                        dorisTable);
                tableMapping.put(cdcTableIdentifier, String.format("%s.%s", targetDb, dorisTable));
                dorisTableIdentifier = tableMapping.get(cdcTableIdentifier);

                JsonNode afterData = recordRoot.get("after");
                if (afterData != null && !afterData.isNull()) {
                    Map<String, Object> stringObjectMap = extractAfterRow(afterData);
                    JsonNode jsonNode = objectMapper.valueToTree(stringObjectMap);

                    PostgresInferredSchema postgresSchema =
                            createPostgresInferredSchema(jsonNode, targetDb, dorisTable);
                    postgresSchema.setModel(DataModel.UNIQUE);
                    DorisTableUtil.tryCreateTableIfAbsent(
                            dorisSystem,
                            targetDb,
                            dorisTable,
                            postgresSchema,
                            changeContext.getDorisTableConf());
                }
            }

            String[] tableInfo = dorisTableIdentifier.split("\\.");
            if (tableInfo.length != 2) {
                throw new DorisRuntimeException(
                        "Invalid table identifier: " + dorisTableIdentifier);
            }
            String database = tableInfo[0];
            String table = tableInfo[1];

            buildDorisTableFieldsMapping(database, table);

            JsonNode afterNode = recordRoot.get("after");
            if (afterNode != null && !afterNode.isNull()) {
                checkAndUpdateSchemaChange(afterNode, dorisTableIdentifier, database, table);
            }

            return true;
        } catch (Exception ex) {
            LOG.warn("Schema change error: ", ex);
            return false;
        }
    }

    private void checkAndUpdateSchemaChange(
            JsonNode afterData, String dorisTableIdentifier, String database, String table) {
        Map<String, String> tableFieldMap = tableFields.get(dorisTableIdentifier);
        if (tableFieldMap == null) {
            LOG.warn("No field mapping found for table {}", dorisTableIdentifier);
            return;
        }

        Set<String> newFields = new HashSet<>();
        afterData
                .fieldNames()
                .forEachRemaining(
                        fieldName -> {
                            if (!tableFieldMap.containsKey(fieldName)) {
                                newFields.add(fieldName);
                            }
                        });

        for (String newField : newFields) {
            try {
                doSchemaChange(newField, afterData, database, table, dorisTableIdentifier);
                LOG.info("Added new column {} to table {}.{}", newField, database, table);
            } catch (Exception e) {
                LOG.error(
                        "Failed to add column {} to table {}.{}: {}",
                        newField,
                        database,
                        table,
                        e.getMessage(),
                        e);
            }
        }
    }

    private void doSchemaChange(
            String fieldName,
            JsonNode afterData,
            String database,
            String table,
            String tableIdentifier)
            throws IOException, IllegalArgumentException {

        JsonNode fieldValue = afterData.get(fieldName);
        String dorisType = PostgresType.jsonNodeToDorisType(fieldValue);

        LOG.info(
                "Adding column {} with type {} to table {}.{}",
                fieldName,
                dorisType,
                database,
                table);

        FieldSchema fieldSchema = new FieldSchema(fieldName, dorisType, null);
        schemaChangeManager.addColumn(database, table, fieldSchema);

        tableFields.get(tableIdentifier).put(fieldName, dorisType);
    }

    private void buildDorisTableFieldsMapping(String databaseName, String tableName) {
        String identifier = databaseName + "." + tableName;
        tableFields.computeIfAbsent(
                identifier, k -> dorisSystem.getTableFieldNames(databaseName, tableName));
    }

    @Override
    public String getCdcTableIdentifier(JsonNode record) {
        JsonNode source = record.get("source");
        if (source == null) {
            LOG.error("Failed to get source from record");
            throw new RuntimeException("Failed to get source from record");
        }
        String schema = extractJsonNode(source, "schema");
        String table = extractJsonNode(source, "table");
        return SourceSchema.getString(null, schema, table);
    }

    @VisibleForTesting
    public void setDorisSystem(DorisSystem dorisSystem) {
        this.dorisSystem = dorisSystem;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> extractAfterRow(JsonNode recordRoot) {
        return objectMapper.convertValue(recordRoot, Map.class);
    }

    private PostgresInferredSchema createPostgresInferredSchema(
            JsonNode jsonNode, String targetDb, String dorisTable) {
        try {
            return new PostgresInferredSchema(jsonNode, targetDb, dorisTable);
        } catch (Exception e) {
            throw new DorisRuntimeException("Failed to create PostgresInferredSchema", e);
        }
    }

    @VisibleForTesting
    public void setSchemaChangeManager(SchemaChangeManager schemaChangeManager) {
        this.schemaChangeManager = schemaChangeManager;
    }

    // Inner class to create PostgreSQL schema from inferred JSON data
    private static class PostgresInferredSchema extends SourceSchema {
        public PostgresInferredSchema(JsonNode jsonNode, String targetDb, String dorisTable)
                throws Exception {
            super(targetDb, null, dorisTable, "");
            this.fields = new java.util.LinkedHashMap<>();
            this.primaryKeys = new java.util.ArrayList<>();
            this.uniqueIndexs = new java.util.ArrayList<>();

            // Add fields based on the JSON data
            jsonNode.fieldNames()
                    .forEachRemaining(
                            fieldName -> {
                                JsonNode fieldValue = jsonNode.get(fieldName);
                                String dorisType = PostgresType.jsonNodeToDorisType(fieldValue);
                                org.apache.doris.flink.catalog.doris.FieldSchema fieldSchema =
                                        new org.apache.doris.flink.catalog.doris.FieldSchema(
                                                fieldName, dorisType, null);
                                this.fields.put(fieldName, fieldSchema);
                            });
        }

        @Override
        public String convertToDorisType(String fieldType, Integer precision, Integer scale) {
            return PostgresType.toDorisType(fieldType, precision, scale);
        }

        @Override
        public String getCdcTableName() {
            return schemaName + "\\." + tableName;
        }
    }
}
