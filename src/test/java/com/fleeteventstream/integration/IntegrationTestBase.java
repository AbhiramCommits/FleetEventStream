package com.fleeteventstream.integration;

import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Starts the real Kafka broker and SQL Server used by the integration tests. Testcontainers
 * pulls the same images as docker-compose.yml, and Flyway migrates the real SQL Server.
 *
 * <p>If Docker is unavailable the whole integration class is skipped (instead of failing),
 * so the unit/slice suite can still run on a laptop without Docker.</p>
 */
@SpringBootTest(properties = {
        "spring.kafka.listener.auto-startup=true",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.jpa.hibernate.ddl-auto=none",
        "fleet.kafka.consumer.concurrency=3",
        "fleet.health.max-lag=10000",
        "fleet.simulator.events-per-second=10",
        "fleet.simulator.vehicle-count=2",
        "management.health.kafka.enabled=false"
})
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Testcontainers(disabledWithoutDocker = true)
public abstract class IntegrationTestBase {

    protected static final String DATABASE_NAME = "fleeteventstream_test";

    @Container
    protected static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.0"))
                    // Kafka 3.9 rejects a controller listener bound to 0.0.0.0. Testcontainers'
                    // 1.20.x default does that, so bind the controller to a routable host.
                    .withEnv("KAFKA_LISTENERS",
                            "PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9093,CONTROLLER://localhost:9094");

    @Container
    @SuppressWarnings("resource")
    protected static final MSSQLServerContainer<?> SQL_SERVER =
            new MSSQLServerContainer<>(DockerImageName.parse("mcr.microsoft.com/mssql/server:2022-latest"))
                    .acceptLicense();

    private static final AtomicBoolean DATABASE_READY = new AtomicBoolean(false);

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url", () -> {
            ensureDatabase();
            return jdbcUrl(DATABASE_NAME);
        });
        registry.add("spring.datasource.username", SQL_SERVER::getUsername);
        registry.add("spring.datasource.password", SQL_SERVER::getPassword);
        registry.add("spring.datasource.hikari.maximum-pool-size", () -> "12");
    }

    private static String jdbcUrl(String database) {
        return "jdbc:sqlserver://" + SQL_SERVER.getHost() + ":" + SQL_SERVER.getMappedPort(1433)
                + ";databaseName=" + database
                + ";encrypt=false;trustServerCertificate=true;loginTimeout=60";
    }

    /**
     * SQL Server containers start in master; create the application database before Flyway
     * opens its first connection.
     */
    private static void ensureDatabase() {
        if (!DATABASE_READY.compareAndSet(false, true)) {
            return;
        }
        try (Connection connection = DriverManager.getConnection(
                jdbcUrl("master"), SQL_SERVER.getUsername(), SQL_SERVER.getPassword());
             Statement statement = connection.createStatement()) {
            statement.execute("IF DB_ID(N'" + DATABASE_NAME + "') IS NULL "
                    + "CREATE DATABASE [" + DATABASE_NAME + "]");
        } catch (SQLException e) {
            DATABASE_READY.set(false);
            throw new IllegalStateException("Could not create test database " + DATABASE_NAME, e);
        }
    }
}
