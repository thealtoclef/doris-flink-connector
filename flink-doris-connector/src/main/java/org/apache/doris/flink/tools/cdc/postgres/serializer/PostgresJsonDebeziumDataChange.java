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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.CdcDataChange;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumChangeContext;
import org.apache.doris.flink.tools.cdc.SourceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.apache.doris.flink.sink.util.DeleteOperation.addDeleteSign;
import static org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumChangeUtils.extractJsonNode;
import static org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumChangeUtils.getDorisTableIdentifier;

public class PostgresJsonDebeziumDataChange extends CdcDataChange {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresJsonDebeziumDataChange.class);

    public DorisOptions dorisOptions;
    public String lineDelimiter;
    public JsonDebeziumChangeContext changeContext;
    public ObjectMapper objectMapper;
    public Map<String, String> tableMapping;
    private final boolean enableDelete;

    public PostgresJsonDebeziumDataChange(JsonDebeziumChangeContext changeContext) {
        this.changeContext = changeContext;
        this.dorisOptions = changeContext.getDorisOptions();
        this.objectMapper = changeContext.getObjectMapper();
        this.lineDelimiter = changeContext.getLineDelimiter();
        this.tableMapping = changeContext.getTableMapping();
        this.enableDelete = changeContext.enableDelete();
    }

    @Override
    public DorisRecord serialize(String record, JsonNode recordRoot, String op) throws IOException {
        String cdcTableIdentifier = getCdcTableIdentifier(recordRoot);
        String dorisTableIdentifier =
                getDorisTableIdentifier(cdcTableIdentifier, dorisOptions, tableMapping);
        if (StringUtils.isNullOrWhitespaceOnly(dorisTableIdentifier)) {
            LOG.warn(
                    "filter table {}, because it is not listened, record detail is {}",
                    cdcTableIdentifier,
                    record);
            return null;
        }
        Map<String, Object> valueMap;
        switch (op) {
            case "r":
            case "c":
            case "u":
                valueMap = extractAfterRow(recordRoot);
                addDeleteSign(valueMap, false);
                break;
            case "d":
                valueMap = extractBeforeRow(recordRoot);
                addDeleteSign(valueMap, enableDelete);
                break;
            default:
                LOG.error("parse record fail, unknown op {} in {}", op, record);
                return null;
        }

        return DorisRecord.of(
                dorisTableIdentifier,
                objectMapper.writeValueAsString(valueMap).getBytes(StandardCharsets.UTF_8));
    }

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

    @Override
    public Map<String, Object> extractBeforeRow(JsonNode record) {
        return extractRowData(record, "before");
    }

    @Override
    public Map<String, Object> extractAfterRow(JsonNode record) {
        return extractRowData(record, "after");
    }

    private Map<String, Object> extractRowData(JsonNode record, String fieldName) {
        JsonNode data = record.get(fieldName);
        if (data == null || data.isNull()) {
            return null;
        }
        return objectMapper.convertValue(data, Map.class);
    }
}
