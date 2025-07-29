# Build stage
FROM maven:3-eclipse-temurin-8 AS builder

WORKDIR /workspace
COPY . .
RUN cd flink-doris-connector && mvn clean package

# Final stage
FROM scratch

# Copy Flink Doris Connector jar from builder
COPY --from=builder /workspace/flink-doris-connector/target/flink-doris-connector-*.jar /flink-doris-connector.jar
