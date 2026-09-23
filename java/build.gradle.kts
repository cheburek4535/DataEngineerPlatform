plugins {
    id("java")
    id("application")
    id("com.gradleup.shadow") version "9.6.1"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:6.0.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.2")
    implementation("org.apache.kafka:kafka-streams:3.7.0")
    implementation("ch.qos.logback:logback-classic:1.5.16")
    implementation("org.postgresql:postgresql:42.7.2")
}

tasks.test {
    useJUnitPlatform()
}
application {
    mainClass = "kafka.integration.WeatherStreamApp"
}