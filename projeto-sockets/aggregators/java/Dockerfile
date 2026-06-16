FROM maven:3.9.7-eclipse-temurin-21 AS build

WORKDIR /build
COPY aggregators/java/pom.xml .
COPY common/ ../common/

# Resolve dependencies
RUN mvn dependency:go-offline

COPY aggregators/java/src/ ./src/
RUN mvn clean package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/target/aggregator-java-1.0-SNAPSHOT.jar ./aggregator.jar
CMD ["java", "-jar", "aggregator.jar"]
