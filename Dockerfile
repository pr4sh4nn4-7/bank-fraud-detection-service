
FROM maven:3.9.16-eclipse-temurin-21-alpine AS builder
WORKDIR /app
COPY pom.xml .

COPY src ./src
RUN mvn clean package -DskipTests

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=builder /app/target/fraud-service.jar ./ 
CMD [ "java" ,"-jar","fraud-service.jar" ]


