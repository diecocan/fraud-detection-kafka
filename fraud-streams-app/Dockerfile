# --- Build stage ---
# NOTE: build context must be the REPO ROOT, not this directory, since the Avro
# codegen needs the sibling ../schemas directory:
#   docker build -f fraud-streams-app/Dockerfile -t <tag> .   (run from repo root)
FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /repo
COPY schemas ./schemas
COPY fraud-streams-app/pom.xml ./fraud-streams-app/pom.xml
COPY fraud-streams-app/src ./fraud-streams-app/src
WORKDIR /repo/fraud-streams-app
RUN mvn clean package -DskipTests

# --- Runtime stage ---
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /repo/fraud-streams-app/target/fraud-detection-kafka-fraud-streams-app-*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
