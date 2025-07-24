# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Development Commands

### Building the Project
- Build with version selection: `cd flink-doris-connector && ./build.sh`
- The build script will prompt for Flink version selection (1.15.x through 1.20.x)
- Maven build directly: `mvn clean package -Dflink.version=1.20.0 -Dflink.major.version=1.20 -Dflink.python.id=flink-python`
- Output JAR is placed in `dist/` directory after successful build

### Code Quality and Testing
- Format code: `mvn spotless:apply` (uses Google Java Format with AOSP style)
- Run checkstyle: `mvn clean compile checkstyle:checkstyle`
- Run tests: `mvn test` (excludes IT and E2E tests by default)
- Full test suite: `mvn verify` (includes integration tests)

### Environment Setup
- Copy `custom_env.sh.tpl` to `custom_env.sh` and configure before building
- Required environment variables are defined in `env.sh`
- Custom Maven repository can be set via `CUSTOM_MAVEN_REPO` environment variable

## Code Architecture

### Core Components
- **Sink Architecture**: `DorisSink` follows Flink's two-phase commit pattern with multiple write modes (STREAM_LOAD, STREAM_LOAD_BATCH, COPY)
- **Source Architecture**: `DorisSource` implements FLIP-27 with split-based parallel reading and automatic partition discovery
- **Table Integration**: Full Flink Table API support via `DorisDynamicTableFactory` with SQL compatibility
- **CDC Tools**: Multi-database CDC support (MySQL, PostgreSQL, Oracle, SQL Server, MongoDB, DB2) with automatic schema evolution

### Key Packages
- `org.apache.doris.flink.sink.*` - Writing data to Doris with various strategies
- `org.apache.doris.flink.source.*` - Reading data from Doris with partition-aware splits
- `org.apache.doris.flink.table.*` - Flink Table API integration and lookup joins
- `org.apache.doris.flink.tools.cdc.*` - Change Data Capture from various databases
- `org.apache.doris.flink.catalog.*` - Flink catalog integration with type mapping
- `org.apache.doris.flink.cfg.*` - Configuration classes with layered options pattern

### Configuration Pattern
All components use builder pattern with layered configuration:
- `DorisOptions` - Basic connection settings (FE/BE nodes, credentials)
- `DorisExecutionOptions` - Write behavior and buffering settings  
- `DorisReadOptions` - Read-specific configurations
- `DorisLookupOptions` - Lookup join configurations

### Serialization System
Pluggable serialization via `DorisRecordSerializer` interface:
- `RowDataSerializer` - Standard Flink RowData format
- `JsonDebeziumSchemaSerializer` - Debezium JSON with schema evolution support
- `PostgresJsonDebeziumSchemaSerializer` - PostgreSQL-specific CDC handling

### Testing Structure
- Unit tests: `src/test/java/org/apache/doris/flink/`
- Integration tests: `*ITCase.java` files (require Doris instance)
- E2E tests: `src/test/java/org/apache/doris/flink/container/e2e/`
- Test containers: Docker-based testing with `DorisContainer` and `MySQLContainer`

## Development Guidelines

### Code Style
- Uses Google Java Format with AOSP style
- Spotless plugin enforces formatting automatically during build
- Import order: `org.apache.flink`, `org.apache.flink.shaded`, others, `javax`, `java`, `scala`, static imports
- Checkstyle configuration in `tools/maven/checkstyle.xml`

### Version Management
- Project supports Flink versions 1.15.x through 1.20.x
- Version properties defined in `pom.xml` with `${flink.version}` and `${flink.major.version}`
- Python dependency ID changes for Flink 1.15.x (`flink-python_2.12` vs `flink-python`)

### Schema Evolution
The connector supports two modes for handling schema changes:
- **Debezium Structure Mode**: Uses Debezium's schema information for DDL operations
- **SQL Parser Mode**: Parses DDL statements directly for schema evolution

### Common Development Patterns
- All major components use builder pattern for configuration
- Two-phase commit pattern for transactional guarantees
- Strategy pattern for multiple write modes and database-specific CDC implementations
- Template method pattern in `DatabaseSync` hierarchy for CDC operations