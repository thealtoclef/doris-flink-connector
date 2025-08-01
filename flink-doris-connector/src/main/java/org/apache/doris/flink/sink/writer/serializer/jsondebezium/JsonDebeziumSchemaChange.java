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

package org.apache.doris.flink.sink.writer.serializer.jsondebezium;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.sink.schema.SchemaChangeManager;
import org.apache.doris.flink.sink.writer.EventType;
import org.apache.doris.flink.tools.cdc.DorisTableConfig;
import org.apache.doris.flink.tools.cdc.SourceConnector;
import org.apache.doris.flink.tools.cdc.SourceSchema;
import org.apache.doris.flink.tools.cdc.converter.TableNameConverter;
import org.apache.doris.flink.tools.cdc.utils.DorisTableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Synchronize the schema change in the upstream data source to the doris database table.
 *
 * <p>{@link SQLParserSchemaChange} Schema Change by parsing upstream DDL.
 */
public abstract class JsonDebeziumSchemaChange extends CdcSchemaChange {
    private static final Logger LOG = LoggerFactory.getLogger(JsonDebeziumSchemaChange.class);
    protected static String addDropDDLRegex =
            "ALTER\\s+TABLE\\s+[^\\s]+\\s+(ADD|DROP)\\s+(COLUMN\\s+)?([^\\s]+)(\\s+([^\\s]+))?.*";
    protected Pattern addDropDDLPattern;

    // table name of the cdc upstream, format is db.tbl
    protected String sourceTableName;
    protected DorisOptions dorisOptions;
    protected ObjectMapper objectMapper;
    // <cdc db.schema.table, doris db.table>
    protected Map<String, String> tableMapping;
    protected SchemaChangeManager schemaChangeManager;
    protected JsonDebeziumChangeContext changeContext;
    protected SourceConnector sourceConnector;
    protected String targetDatabase;
    protected String targetTablePrefix;
    protected String targetTableSuffix;
    protected DorisTableConfig dorisTableConfig;
    protected TableNameConverter tableNameConverter;
    protected final boolean enableDelete;

    public JsonDebeziumSchemaChange(JsonDebeziumChangeContext changeContext) {
        this.changeContext = changeContext;
        this.dorisOptions = changeContext.getDorisOptions();
        this.schemaChangeManager = new SchemaChangeManager(dorisOptions);
        this.tableMapping = changeContext.getTableMapping();
        this.objectMapper = changeContext.getObjectMapper();
        this.targetDatabase = changeContext.getTargetDatabase();
        this.dorisTableConfig = changeContext.getDorisTableConf();
        this.targetTablePrefix =
                changeContext.getTargetTablePrefix() == null
                        ? ""
                        : changeContext.getTargetTablePrefix();
        this.targetTableSuffix =
                changeContext.getTargetTableSuffix() == null
                        ? ""
                        : changeContext.getTargetTableSuffix();
        this.tableNameConverter = changeContext.getTableNameConverter();
        this.enableDelete = changeContext.enableDelete();
    }

    public abstract boolean schemaChange(JsonNode recordRoot);

    public void init(JsonNode recordRoot, String dorisTableName) {
        ensureCdcTimestampColumns(dorisTableName);
    }

    /** When cdc synchronizes multiple tables, it will capture multiple table schema changes. */
    protected boolean checkTable(JsonNode recordRoot) {
        String db = extractDatabase(recordRoot);
        String tbl = extractTable(recordRoot);
        String dbTbl = db + "." + tbl;
        return sourceTableName.equals(dbTbl);
    }

    @Override
    protected String extractDatabase(JsonNode record) {
        if (record.get("source").has("schema")) {
            // compatible with schema
            return extractJsonNode(record.get("source"), "schema");
        } else {
            return extractJsonNode(record.get("source"), "db");
        }
    }

    @Override
    protected String extractTable(JsonNode record) {
        return extractJsonNode(record.get("source"), "table");
    }

    protected String extractJsonNode(JsonNode record, String key) {
        return record != null && record.get(key) != null && !(record.get(key) instanceof NullNode)
                ? record.get(key).asText()
                : null;
    }

    /**
     * Parse doris database and table as a tuple.
     *
     * @param record from flink cdc.
     * @return Tuple(database, table)
     */
    protected Tuple2<String, String> getDorisTableTuple(JsonNode record) {
        String identifier =
                JsonDebeziumChangeUtils.getDorisTableIdentifier(record, dorisOptions, tableMapping);
        if (StringUtils.isNullOrWhitespaceOnly(identifier)) {
            return null;
        }
        String[] tableInfo = identifier.split("\\.");
        if (tableInfo.length != 2) {
            return null;
        }
        return Tuple2.of(tableInfo[0], tableInfo[1]);
    }

    @VisibleForTesting
    @Override
    public String getCdcTableIdentifier(JsonNode record) {
        String db = extractJsonNode(record.get("source"), "db");
        String schema = extractJsonNode(record.get("source"), "schema");
        String table = extractJsonNode(record.get("source"), "table");
        return SourceSchema.getString(db, schema, table);
    }

    protected JsonNode extractHistoryRecord(JsonNode record) throws JsonProcessingException {
        if (record != null && record.has("historyRecord")) {
            return objectMapper.readTree(record.get("historyRecord").asText());
        }
        // The ddl passed by some scenes will not be included in the historyRecord,
        // such as DebeziumSourceFunction
        return record;
    }

    /** Parse event type. */
    protected EventType extractEventType(JsonNode record) throws JsonProcessingException {
        JsonNode tableChange = extractTableChange(record);
        if (tableChange == null || tableChange.get("type") == null) {
            return null;
        }
        String type = tableChange.get("type").asText();
        if (EventType.ALTER.toString().equalsIgnoreCase(type)) {
            return EventType.ALTER;
        } else if (EventType.CREATE.toString().equalsIgnoreCase(type)) {
            return EventType.CREATE;
        }
        LOG.warn("Not supported this event type. type={}", type);
        return null;
    }

    protected JsonNode extractTableChange(JsonNode record) throws JsonProcessingException {
        JsonNode historyRecord = extractHistoryRecord(record);
        JsonNode tableChanges = historyRecord.get("tableChanges");
        if (Objects.nonNull(tableChanges)) {
            return tableChanges.get(0);
        }
        LOG.warn("Failed to extract tableChanges. record={}", record);
        return null;
    }

    protected boolean executeAlterDDLs(
            List<String> ddlSqlList,
            JsonNode recordRoot,
            Tuple2<String, String> dorisTableTuple,
            boolean status)
            throws IOException, IllegalArgumentException {
        if (CollectionUtils.isEmpty(ddlSqlList)) {
            LOG.info("The recordRoot cannot extract ddl sql. recordRoot={}", recordRoot);
            return false;
        }

        for (String ddlSql : ddlSqlList) {
            status = schemaChangeManager.execute(ddlSql, dorisTableTuple.f0);
            LOG.info("schema change status:{}, ddl: {}", status, ddlSql);
        }
        return status;
    }

    protected void extractSourceConnector(JsonNode record) {
        if (Objects.isNull(sourceConnector)) {
            sourceConnector =
                    SourceConnector.valueOf(
                            record.get("source").get("connector").asText().toUpperCase());
        }
    }

    protected String getCreateTableIdentifier(JsonNode record) {
        String table = extractJsonNode(record.get("source"), "table");
        String createTblName;
        if (tableNameConverter != null) {
            createTblName = tableNameConverter.convert(table);
        } else {
            createTblName = targetTablePrefix + table + targetTableSuffix;
        }
        return targetDatabase + "." + createTblName;
    }

    public Map<String, String> getTableMapping() {
        return tableMapping;
    }

    @VisibleForTesting
    public void setSchemaChangeManager(SchemaChangeManager schemaChangeManager) {
        this.schemaChangeManager = schemaChangeManager;
    }

    /**
     * Ensure that CDC timestamp columns exist in the Doris table. This method checks if the
     * required timestamp columns exist in the target table, and automatically adds them if they are
     * missing. This handles cases where the connector is used with existing Doris tables that were
     * created before timestamp column support.
     *
     * @param dorisTableName The full table identifier in format "database.table"
     * @throws DorisRuntimeException if timestamp columns cannot be added or any other error occurs
     */
    private void ensureCdcTimestampColumns(String dorisTableName) {
        // Only handle null/empty table name gracefully - this is expected in some cases
        if (StringUtils.isNullOrWhitespaceOnly(dorisTableName)) {
            LOG.debug("Table name is null or empty, skipping CDC timestamp column check");
            return;
        }

        // All other errors should fail fast - these are configuration/infrastructure issues
        String[] tableInfo = dorisTableName.split("\\.");
        if (tableInfo.length != 2) {
            String errorMsg =
                    String.format(
                            "Invalid table identifier format: %s, expected 'database.table'",
                            dorisTableName);
            LOG.error(errorMsg);
            throw new DorisRuntimeException(errorMsg);
        }

        String database = tableInfo[0];
        String table = tableInfo[1];

        try {
            // Get the CDC timestamp field definitions from our utility
            Map<String, FieldSchema> timestampFields = new HashMap<>();
            DorisTableUtil.addCdcTimestampFields(timestampFields);

            // Check and add each timestamp column if missing
            for (Map.Entry<String, FieldSchema> entry : timestampFields.entrySet()) {
                String columnName = entry.getKey();
                FieldSchema fieldSchema = entry.getValue();

                if (!schemaChangeManager.checkColumnExists(database, table, columnName)) {
                    boolean success = schemaChangeManager.addColumn(database, table, fieldSchema);
                    if (success) {
                        LOG.info(
                                "Added {} column to existing table {}", columnName, dorisTableName);
                    } else {
                        String errorMsg =
                                String.format(
                                        "Failed to add required CDC timestamp column %s to table %s. "
                                                + "This column is essential for CDC functionality.",
                                        columnName, dorisTableName);
                        LOG.error(errorMsg);
                        throw new DorisRuntimeException(errorMsg);
                    }
                } else {
                    LOG.debug(
                            "CDC timestamp column {} already exists in table {}",
                            columnName,
                            dorisTableName);
                }
            }

        } catch (DorisRuntimeException e) {
            // Re-throw DorisRuntimeException (from failed column addition)
            throw e;
        } catch (Exception e) {
            String errorMsg =
                    String.format(
                            "Failed to ensure CDC timestamp columns for table %s: %s. "
                                    + "This is required for CDC functionality to work correctly.",
                            dorisTableName, e.getMessage());
            LOG.error(errorMsg, e);
            throw new DorisRuntimeException(errorMsg, e);
        }
    }
}
