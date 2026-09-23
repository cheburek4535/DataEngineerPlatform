FROM gradle:9.7.1-jdk-25-and-26-corretto AS builder
WORKDIR /app

COPY gradle gradle
COPY gradlew build.gradle.kts settings.gradle.kts* ./
RUN gradle dependencies --no-daemon || true

COPY src src
RUN gradle shadowJar --no-daemon

RUN mv $(ls build/libs/*.jar | grep -v -- '-plain.jar' | head -n 1) build/libs/app.jar

FROM eclipse-temurin:25-jre-alpine
WORKDIR /app

RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

COPY --from=builder /app/build/libs/app.jar app.jar

ENTRYPOINT ["java", "-jar", "app.jar"]
