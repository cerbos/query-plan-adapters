plugins {
    java
}

group = "dev.cerbos"
version = "0.1.0-alpha.1"

// `options.release` (unlike source/targetCompatibility) also constrains the JDK API
// surface: compiling on JDK 21/25 still resolves against the Java 17 class library, so a
// stray post-17 API reference fails at compile time instead of with NoSuchMethodError on
// a JDK 17 runtime.
tasks.withType<JavaCompile> {
    options.release = 17
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("dev.cerbos:cerbos-sdk-java:0.18.0")
    // Must match the gencode version cerbos-sdk-java was generated against (see the README
    // "Pin protobuf-java" gotcha) — older runtimes throw ProtobufRuntimeVersionException.
    implementation("com.google.protobuf:protobuf-java:4.33.5")
    // Spring Data JPA + Jakarta Persistence are provided by the consuming application's
    // Spring Boot BOM (or equivalent). Declaring them as `compileOnly` keeps them out of
    // the published POM as transitive dependencies so they don't pin a specific version on
    // downstream consumers — matching how Spring Data JPA itself marks `hibernate-core`
    // as `<optional>true</optional>`.
    compileOnly("org.springframework.data:spring-data-jpa:3.5.1")
    compileOnly("jakarta.persistence:jakarta.persistence-api:3.2.0")
    // Hibernate is needed only to compile MySqlDoubleCastFunctionContributor (the MySQL
    // IEEE double-cast registration) and the adapter's classpath-guarded probe for it.
    // `compileOnly` for the same reason as Spring Data JPA above: the consuming
    // application provides its own Hibernate, and the adapter degrades gracefully (plain
    // cb.toDouble casts) when Hibernate is absent at runtime.
    compileOnly("org.hibernate.orm:hibernate-core:6.6.18.Final")

    testImplementation("org.springframework.data:spring-data-jpa:3.5.1")
    testImplementation("jakarta.persistence:jakarta.persistence-api:3.2.0")
    testImplementation(platform("org.junit:junit-bom:5.12.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.testcontainers:testcontainers:1.21.3")
    testImplementation("org.testcontainers:junit-jupiter:1.21.3")
    // Real-database legs for AdversarialConformanceTest (selected via ADAPTER_TEST_DB /
    // -Dadapter.test.db): PostgreSQL and MySQL containers + their JDBC drivers.
    testImplementation("org.testcontainers:postgresql:1.21.3")
    testImplementation("org.testcontainers:mysql:1.21.3")
    testRuntimeOnly("org.postgresql:postgresql:42.7.7")
    testRuntimeOnly("com.mysql:mysql-connector-j:9.3.0")
    testImplementation("org.hibernate.orm:hibernate-core:6.6.18.Final")
    testImplementation("com.h2database:h2:2.3.232")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.17")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = false
    }
    // Propagate Cerbos PDP connection details to the test JVM so SpringDataIntegrationTest can
    // choose between the Testcontainers-managed PDP (default) and an externally-managed one
    // (e.g. spawned by docker-compose for CI / `scripts/run-e2e.sh`).
    val cerbosHost = System.getenv("CERBOS_HOST")
    val cerbosPort = System.getenv("CERBOS_PORT")
    if (cerbosHost != null) {
        environment("CERBOS_HOST", cerbosHost)
    }
    if (cerbosPort != null) {
        environment("CERBOS_PORT", cerbosPort)
    }
    // Select the database backing AdversarialConformanceTest: h2 (default), postgres, or
    // mysql. The MySQL leg creates its schema with a case-sensitive collation by default
    // (utf8mb4_0900_as_cs); override adapter.test.mysql.collation to reproduce the
    // over-grant on MySQL's default utf8mb4_0900_ai_ci — see the README
    // "Database collation requirements" section.
    val adapterTestDb = System.getProperty("adapter.test.db") ?: System.getenv("ADAPTER_TEST_DB")
    if (adapterTestDb != null) {
        systemProperty("adapter.test.db", adapterTestDb)
    }
    val mysqlCollation = System.getProperty("adapter.test.mysql.collation")
        ?: System.getenv("ADAPTER_TEST_MYSQL_COLLATION")
    if (mysqlCollation != null) {
        systemProperty("adapter.test.mysql.collation", mysqlCollation)
    }
    // The MySQL leg runs with Connector/J's default CLIENT-side prepared statements so the
    // differential oracle pins the adapter's `cast(... as double)` rendering (see the README
    // "MySQL: keeping arithmetic IEEE-faithful" gotcha). Set this to true to run the same
    // leg with server-side prepared statements — both modes must pass.
    val mysqlServerPrep = System.getProperty("adapter.test.mysql.serverPrepStmts")
        ?: System.getenv("ADAPTER_TEST_MYSQL_SERVER_PREP_STMTS")
    if (mysqlServerPrep != null) {
        systemProperty("adapter.test.mysql.serverPrepStmts", mysqlServerPrep)
    }
}
