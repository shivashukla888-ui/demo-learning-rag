FROM maven:3.9-eclipse-temurin-21
WORKDIR /app
COPY pom.xml .
RUN mvn -q -DskipTests dependency:go-offline
COPY src src
COPY config config
RUN mvn -q -DskipTests package
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "target/surveillance-api-0.9.0.jar"]
