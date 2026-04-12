plugins {
    application
}

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.logging.log4j:log4j-api:2.25.4")
    implementation("org.apache.logging.log4j:log4j-core:2.25.4")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.25.4")

    runtimeOnly("org.postgresql:postgresql:42.7.7")
    implementation("org.flywaydb:flyway-core:11.13.0")
    runtimeOnly("org.flywaydb:flyway-database-postgresql:11.13.0")
    implementation("com.zaxxer:HikariCP:7.0.2")

    testImplementation("org.junit.jupiter:junit-jupiter:5.13.4")
    testImplementation("org.testcontainers:testcontainers:1.21.3")
    testImplementation("org.testcontainers:junit-jupiter:1.21.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.13.4")
}

application {
    mainClass.set("ai.toloka.engineering.pg_queue_buffer.Main")
}

tasks.test {
    useJUnitPlatform()
}
