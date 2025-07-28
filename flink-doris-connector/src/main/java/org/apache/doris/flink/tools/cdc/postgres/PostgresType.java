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

package org.apache.doris.flink.tools.cdc.postgres;

import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.doris.flink.catalog.doris.DorisType;

public class PostgresType {
    private static final String INT2 = "int2";
    private static final String SMALLSERIAL = "smallserial";
    private static final String INT4 = "int4";
    private static final String SERIAL = "serial";
    private static final String INT8 = "int8";
    private static final String BIGSERIAL = "bigserial";
    private static final String NUMERIC = "numeric";
    private static final String FLOAT4 = "float4";
    private static final String FLOAT8 = "float8";
    private static final String BPCHAR = "bpchar";
    private static final String TIMESTAMP = "timestamp";
    private static final String TIMESTAMPTZ = "timestamptz";
    private static final String DATE = "date";
    private static final String BOOL = "bool";
    private static final String BIT = "bit";
    private static final String POINT = "point";
    private static final String LINE = "line";
    private static final String LSEG = "lseg";
    private static final String BOX = "box";
    private static final String PATH = "path";
    private static final String POLYGON = "polygon";
    private static final String CIRCLE = "circle";
    private static final String VARCHAR = "varchar";
    private static final String TEXT = "text";
    private static final String TIME = "time";
    private static final String TIMETZ = "timetz";
    private static final String INTERVAL = "interval";
    private static final String CIDR = "cidr";
    private static final String INET = "inet";
    private static final String MACADDR = "macaddr";
    private static final String VARBIT = "varbit";
    private static final String UUID = "uuid";
    private static final String BYTEA = "bytea";
    private static final String JSON = "json";
    private static final String JSONB = "jsonb";
    private static final String _INT2 = "_int2";
    private static final String _INT4 = "_int4";
    private static final String _INT8 = "_int8";
    private static final String _FLOAT4 = "_float4";
    private static final String _FLOAT8 = "_float8";
    private static final String _DATE = "_date";
    private static final String _TIMESTAMP = "_timestamp";
    private static final String _BOOL = "_bool";
    private static final String _TEXT = "_text";

    public static String toDorisType(String postgresType, Integer precision, Integer scale) {
        postgresType = postgresType.toLowerCase();
        if (postgresType.startsWith("_")) {
            return DorisType.STRING;
        }
        switch (postgresType) {
            case INT2:
            case SMALLSERIAL:
                return DorisType.SMALLINT;
            case INT4:
            case SERIAL:
                return DorisType.INT;
            case INT8:
            case BIGSERIAL:
                return DorisType.BIGINT;
            case NUMERIC:
                return precision != null && precision > 0 && precision <= 38
                        ? String.format(
                                "%s(%s,%s)",
                                DorisType.DECIMAL_V3,
                                precision,
                                scale != null && scale >= 0 ? scale : 0)
                        : DorisType.STRING;
            case FLOAT4:
                return DorisType.FLOAT;
            case FLOAT8:
                return DorisType.DOUBLE;
            case TIMESTAMP:
            case TIMESTAMPTZ:
                return String.format(
                        "%s(%s)", DorisType.DATETIME_V2, Math.min(scale == null ? 0 : scale, 6));
            case DATE:
                return DorisType.DATE_V2;
            case BOOL:
                return DorisType.BOOLEAN;
            case BIT:
                return precision == 1 ? DorisType.BOOLEAN : DorisType.STRING;
            case BPCHAR:
            case VARCHAR:
                Preconditions.checkNotNull(precision);
                return precision * 3 > 65533
                        ? DorisType.STRING
                        : String.format("%s(%s)", DorisType.VARCHAR, precision * 3);
            case POINT:
            case LINE:
            case LSEG:
            case BOX:
            case PATH:
            case POLYGON:
            case CIRCLE:
            case TEXT:
            case TIME:
            case TIMETZ:
            case INTERVAL:
            case CIDR:
            case INET:
            case MACADDR:
            case VARBIT:
            case UUID:
            case BYTEA:
                return DorisType.STRING;
            case JSON:
            case JSONB:
                return DorisType.VARIANT;
                /* Compatible with doris1.2 array type can only be used in dup table,
                   and then converted to array in the next version
                case _BOOL:
                    return String.format("%s<%s>", DorisType.ARRAY, DorisType.BOOLEAN);
                case _INT2:
                    return String.format("%s<%s>", DorisType.ARRAY, DorisType.TINYINT);
                case _INT4:
                    return String.format("%s<%s>", DorisType.ARRAY, DorisType.INT);
                case _INT8:
                    return String.format("%s<%s>", DorisType.ARRAY, DorisType.BIGINT);
                case _FLOAT4:
                    return String.format("%s<%s>", DorisType.ARRAY, DorisType.FLOAT);
                case _FLOAT8:
                    return String.format("%s<%s>", DorisType.ARRAY, DorisType.DOUBLE);
                case _TEXT:
                    return String.format("%s<%s>", DorisType.ARRAY, DorisType.STRING);
                case _DATE:
                    return String.format("%s<%s>", DorisType.ARRAY, DorisType.DATE_V2);
                case _TIMESTAMP:
                    return String.format("%s<%s>", DorisType.ARRAY, DorisType.DATETIME_V2);
                **/
            default:
                return DorisType.VARIANT;
        }
    }

    /**
     * Infer Doris type from JsonNode value for schema evolution. This method is used when
     * PostgreSQL logical replication doesn't provide DDL events and we need to deduce column types
     * from actual data values. We use loose/conservative typing since we're inferring from single
     * values, which may not represent the full range of data in the column.
     */
    public static String jsonNodeToDorisType(JsonNode value) {
        if (value == null || value.isNull()) {
            // For null values, use VARIANT as a catch-all
            return DorisType.VARIANT;
        }

        if (value instanceof IntNode || value instanceof LongNode) {
            // Use BIGINT as loose type to accommodate any integer values
            return DorisType.BIGINT;
        } else if (value instanceof FloatNode || value instanceof DoubleNode) {
            // Use DOUBLE as loose type for floating point numbers
            return DorisType.DOUBLE;
        } else if (value instanceof DecimalNode) {
            // Use loose DECIMAL_V3 with maximum supported precision
            return DorisType.DECIMAL_V3 + "(38,9)";
        } else if (value instanceof BooleanNode) {
            return DorisType.BOOLEAN;
        } else if (value instanceof TextNode) {
            String textValue = value.asText();
            return inferStringType(textValue);
        } else if (value instanceof ArrayNode || value instanceof ObjectNode) {
            // Use VARIANT for arrays and objects
            return DorisType.VARIANT;
        } else {
            // For all other types, use VARIANT as a catch-all
            return DorisType.VARIANT;
        }
    }

    private static String inferStringType(String textValue) {
        if (textValue == null || textValue.isEmpty()) {
            // For empty strings, use STRING as a catch-all
            return DorisType.STRING;
        }

        // Check for clear timestamp patterns (conservative)
        if (isTimestampLike(textValue)) {
            return DorisType.DATETIME_V2 + "(6)";
        }

        // Check for clear date patterns
        if (isDateLike(textValue)) {
            return DorisType.DATE_V2;
        }

        // Check for JSON patterns - these can benefit from VARIANT type
        if (isJsonLike(textValue)) {
            return DorisType.VARIANT;
        }

        // For all other strings, use STRING type (most flexible)
        // This includes UUIDs, times, and any other string data
        return DorisType.STRING;
    }

    private static boolean isTimestampLike(String value) {
        // Match ISO 8601 timestamps: YYYY-MM-DD HH:MM:SS or YYYY-MM-DDTHH:MM:SS
        return value.matches("\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2}.*");
    }

    private static boolean isDateLike(String value) {
        // Match ISO date format: YYYY-MM-DD
        return value.matches("\\d{4}-\\d{2}-\\d{2}");
    }

    private static boolean isJsonLike(String value) {
        // Basic JSON pattern detection
        value = value.trim();
        return (value.startsWith("{") && value.endsWith("}"))
                || (value.startsWith("[") && value.endsWith("]"));
    }
}
