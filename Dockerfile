# Build stage
FROM maven:3-eclipse-temurin-8 AS builder

WORKDIR /workspace
COPY . .
RUN cd flink-doris-connector && \
    echo "6" | bash build.sh

# Final stage
FROM scratch

# Copy Flink Doris Connector jar from builder
COPY --from=builder /workspace/dist/flink-doris-*.jar /flink-doris-connector.jar
