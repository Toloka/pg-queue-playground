plugins {
    application
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.logging.log4j:log4j-api:2.19.0")
    implementation("org.apache.logging.log4j:log4j-core:2.19.0")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.19.0")


    implementation("org.postgresql:postgresql:42.5.1")
    implementation("org.flywaydb:flyway-core:9.8.3")
    implementation("com.zaxxer:HikariCP:5.0.1")

    testImplementation("org.junit.jupiter:junit-jupiter:5.9.0")
    testImplementation("org.testcontainers:testcontainers:1.17.6")
    testImplementation("org.testcontainers:junit-jupiter:1.17.6")
}

application {
    mainClass.set("ai.toloka.engineering.pg_queue_buffer.Main")
}

tasks.test {
    useJUnitPlatform()
}
