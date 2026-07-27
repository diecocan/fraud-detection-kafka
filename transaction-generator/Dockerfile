# --- Build stage ---
# NOTE: build context must be the REPO ROOT, not this directory, since the Avro
# codegen needs the sibling ../schemas directory:
#   docker build -f transaction-generator/Dockerfile -t <tag> .   (run from repo root)
FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /repo
COPY schemas ./schemas
COPY transaction-generator/pom.xml ./transaction-generator/pom.xml
COPY transaction-generator/src ./transaction-generator/src
WORKDIR /repo/transaction-generator
RUN mvn clean package -DskipTests

# --- Runtime stage ---
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /repo/transaction-generator/target/fraud-detection-kafka-transaction-generator-*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
