# --- Build stage ---
# NOTE: build context must be the REPO ROOT, not this directory, since the Avro
# codegen needs the sibling ../schemas directory:
#   docker build -f alert-consumer-api/Dockerfile -t <tag> .   (run from repo root)
FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /repo
COPY schemas ./schemas
COPY alert-consumer-api/pom.xml ./alert-consumer-api/pom.xml
COPY alert-consumer-api/src ./alert-consumer-api/src
WORKDIR /repo/alert-consumer-api
RUN mvn clean package -DskipTests

# --- Runtime stage ---
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /repo/alert-consumer-api/target/fraud-detection-kafka-alert-consumer-api-*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
