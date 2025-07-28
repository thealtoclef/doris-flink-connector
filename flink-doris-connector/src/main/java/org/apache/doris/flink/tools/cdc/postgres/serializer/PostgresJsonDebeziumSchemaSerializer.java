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

import org.apache.flink.util.StringUtils;

import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumChangeContext;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumChangeUtils;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumDataChange;
import org.apache.doris.flink.tools.cdc.DorisTableConfig;
import org.apache.doris.flink.tools.cdc.converter.TableNameConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.apache.doris.flink.sink.writer.LoadConstants.LINE_DELIMITER_DEFAULT;
import static org.apache.doris.flink.sink.writer.LoadConstants.LINE_DELIMITER_KEY;

public class PostgresJsonDebeziumSchemaSerializer implements DorisRecordSerializer<String> {
    private static final Logger LOG =
            LoggerFactory.getLogger(PostgresJsonDebeziumSchemaSerializer.class);
    private final Pattern pattern;
    private final DorisOptions dorisOptions;
    private final ObjectMapper objectMapper = new ObjectMapper();
    // table name of the cdc upstream, format is db.tbl
    private final String sourceTableName;
    private String lineDelimiter = LINE_DELIMITER_DEFAULT;
    private boolean ignoreUpdateBefore = true;
    private boolean enableDelete = true;
    private boolean enableDrop = true;
    // <cdc db.schema.table, doris db.table>
    private Map<String, String> tableMapping;
    // create table properties
    private DorisTableConfig dorisTableConfig;
    private String targetDatabase;
    private String targetTablePrefix;
    private String targetTableSuffix;
    private JsonDebeziumDataChange dataChange;
    private PostgresJsonDebeziumSchemaChange schemaChange;
    private TableNameConverter tableNameConverter;
    // jdbc url of the source database
    private String jdbcUrl;
    private Properties connectionProps;

    public PostgresJsonDebeziumSchemaSerializer(
            DorisOptions dorisOptions,
            Pattern pattern,
            String sourceTableName,
            DorisExecutionOptions executionOptions,
            Map<String, String> tableMapping,
            DorisTableConfig dorisTableConfig,
            String targetDatabase,
            String targetTablePrefix,
            String targetTableSuffix,
            TableNameConverter tableNameConverter,
            String jdbcUrl,
            Properties connectionProps) {
        this.dorisOptions = dorisOptions;
        this.pattern = pattern;
        this.sourceTableName = sourceTableName;
        // Prevent loss of decimal data precision
        JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(true);
        objectMapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
        objectMapper.configure(Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
        objectMapper.setNodeFactory(jsonNodeFactory);

        if (executionOptions != null) {
            this.lineDelimiter =
                    executionOptions
                            .getStreamLoadProp()
                            .getProperty(LINE_DELIMITER_KEY, LINE_DELIMITER_DEFAULT);
            this.ignoreUpdateBefore = executionOptions.getIgnoreUpdateBefore();
            this.enableDelete = executionOptions.getDeletable();
            this.enableDrop = executionOptions.getDropable();
        }

        this.tableMapping = tableMapping;
        this.targetDatabase = targetDatabase;
        this.targetTablePrefix = targetTablePrefix;
        this.targetTableSuffix = targetTableSuffix;
        this.dorisTableConfig = dorisTableConfig;
        this.tableNameConverter = tableNameConverter;
        this.jdbcUrl = jdbcUrl;
        this.connectionProps = connectionProps;
        init();
    }

    private void init() {
        JsonDebeziumChangeContext changeContext =
                new JsonDebeziumChangeContext(
                        dorisOptions,
                        tableMapping,
                        sourceTableName,
                        targetDatabase,
                        dorisTableConfig,
                        objectMapper,
                        pattern,
                        lineDelimiter,
                        ignoreUpdateBefore,
                        targetTablePrefix,
                        targetTableSuffix,
                        enableDelete,
                        enableDrop,
                        tableNameConverter);
        this.dataChange = new JsonDebeziumDataChange(changeContext);
        this.schemaChange =
                new PostgresJsonDebeziumSchemaChange(changeContext, jdbcUrl, connectionProps);
    }

    @Override
    public DorisRecord serialize(String record) throws IOException {
        LOG.debug("received debezium json data {} :", record);
        JsonNode recordRoot = objectMapper.readValue(record, JsonNode.class);
        String op = extractJsonNode(recordRoot, "op");

        this.tableMapping = schemaChange.getTableMapping();
        String dorisTableName =
                JsonDebeziumChangeUtils.getDorisTableIdentifier(
                        recordRoot, dorisOptions, tableMapping);
        if (StringUtils.isNullOrWhitespaceOnly(dorisTableName)) {
            // Auto-discovery
            schemaChange.init(recordRoot, dorisTableName);
        }

        schemaChange.schemaChange(recordRoot);

        return dataChange.serialize(record, recordRoot, op);
    }

    private String extractJsonNode(JsonNode record, String key) {
        return record != null && record.get(key) != null && !(record.get(key) instanceof NullNode)
                ? record.get(key).asText()
                : null;
    }

    public static PostgresJsonDebeziumSchemaSerializer.Builder builder() {
        return new PostgresJsonDebeziumSchemaSerializer.Builder();
    }

    /** Builder for PostgresJsonDebeziumSchemaSerializer. */
    public static class Builder {
        private DorisOptions dorisOptions;
        private Pattern addDropDDLPattern;
        private String sourceTableName;
        private DorisExecutionOptions executionOptions;
        private Map<String, String> tableMapping;
        private DorisTableConfig dorisTableConfig;
        private String targetDatabase;
        private String targetTablePrefix = "";
        private String targetTableSuffix = "";
        private TableNameConverter tableNameConverter;
        private String jdbcUrl;
        private Properties connectionProps;

        public PostgresJsonDebeziumSchemaSerializer.Builder setDorisOptions(
                DorisOptions dorisOptions) {
            this.dorisOptions = dorisOptions;
            return this;
        }

        public PostgresJsonDebeziumSchemaSerializer.Builder setPattern(Pattern addDropDDLPattern) {
            this.addDropDDLPattern = addDropDDLPattern;
            return this;
        }

        public PostgresJsonDebeziumSchemaSerializer.Builder setSourceTableName(
                String sourceTableName) {
            this.sourceTableName = sourceTableName;
            return this;
        }

        public PostgresJsonDebeziumSchemaSerializer.Builder setExecutionOptions(
                DorisExecutionOptions executionOptions) {
            this.executionOptions = executionOptions;
            return this;
        }

        public PostgresJsonDebeziumSchemaSerializer.Builder setTableMapping(
                Map<String, String> tableMapping) {
            this.tableMapping = tableMapping;
            return this;
        }

        @Deprecated
        public PostgresJsonDebeziumSchemaSerializer.Builder setTableProperties(
                Map<String, String> tableProperties) {
            this.dorisTableConfig = new DorisTableConfig(tableProperties);
            return this;
        }

        public PostgresJsonDebeziumSchemaSerializer.Builder setDorisTableConf(
                DorisTableConfig dorisTableConfig) {
            this.dorisTableConfig = dorisTableConfig;
            return this;
        }

        public PostgresJsonDebeziumSchemaSerializer.Builder setTargetDatabase(
                String targetDatabase) {
            this.targetDatabase = targetDatabase;
            return this;
        }

        public PostgresJsonDebeziumSchemaSerializer.Builder setTargetTablePrefix(
                String targetTablePrefix) {
            this.targetTablePrefix = targetTablePrefix;
            return this;
        }

        public PostgresJsonDebeziumSchemaSerializer.Builder setTargetTableSuffix(
                String targetTableSuffix) {
            this.targetTableSuffix = targetTableSuffix;
            return this;
        }

        public PostgresJsonDebeziumSchemaSerializer.Builder setTableNameConverter(
                TableNameConverter converter) {
            this.tableNameConverter = converter;
            return this;
        }

        public PostgresJsonDebeziumSchemaSerializer.Builder setJdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        public PostgresJsonDebeziumSchemaSerializer.Builder setConnectionProps(
                Properties connectionProps) {
            this.connectionProps = connectionProps;
            return this;
        }

        public PostgresJsonDebeziumSchemaSerializer build() {
            return new PostgresJsonDebeziumSchemaSerializer(
                    dorisOptions,
                    addDropDDLPattern,
                    sourceTableName,
                    executionOptions,
                    tableMapping,
                    dorisTableConfig,
                    targetDatabase,
                    targetTablePrefix,
                    targetTableSuffix,
                    tableNameConverter,
                    jdbcUrl,
                    connectionProps);
        }
    }
}
