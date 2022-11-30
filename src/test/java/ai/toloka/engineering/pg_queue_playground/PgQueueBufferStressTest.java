package ai.toloka.engineering.pg_queue_playground;

import java.io.File;

import ai.toloka.engineering.pg_queue_playground.misc.StressTestConfig;
import ai.toloka.engineering.pg_queue_playground.misc.StressTestRunner;
import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@TestMethodOrder(MethodOrderer.MethodName.class)
class PgQueueBufferStressTest {

    @SuppressWarnings("resource")
    @ClassRule
    public static DockerComposeContainer<?> compose =
            new DockerComposeContainer<>(new File("docker-compose.yml"))
                    .withExposedService("pg-primary", 5432);

    public static String host;
    public static Integer port;

    @BeforeAll
    public static void init() {
        compose.start();
        host = compose.getServiceHost("pg-primary", 5432);
        port = compose.getServicePort("pg-primary", 5432);
    }

    @Test
    void test_PgQueueBuffer_1_SelectForUpdate() {
        StressTestConfig config = StressTestConfig.builder()
                .setPgQueueBuffer(new PgQueueBuffer_1_SelectForUpdate())
                .setWriterCount(70)
                .setWriterInnerDelayMs(10)
                .setReaderCount(5)
                .setReaderInnerDelayMs(50)
                .setReaderBatchSize(40)
                .setDurationSec(300)
                .setLongTxEnabled(true)
                .setHost(host)
                .setPort(port)
                .build();

        StressTestRunner.run(config);
    }

    @Test
    void test_PgQueueBuffer_2_SelectForUpdateSkipLocked() {
        StressTestConfig config = StressTestConfig.builder()
                .setPgQueueBuffer(new PgQueueBuffer_2_SelectForUpdateSkipLocked())
                .setWriterCount(70)
                .setWriterInnerDelayMs(10)
                .setReaderCount(5)
                .setReaderInnerDelayMs(50)
                .setReaderBatchSize(40)
                .setDurationSec(300)
                .setLongTxEnabled(true)
                .setHost(host)
                .setPort(port)
                .build();

        StressTestRunner.run(config);
    }

    @Test
    void test_PgQueueBuffer_3_SelectForUpdateSkipLockedWhereId() {
        StressTestConfig config = StressTestConfig.builder()
                .setPgQueueBuffer(new PgQueueBuffer_3_SelectForUpdateSkipLockedWhereId(10))
                .setWriterCount(70)
                .setWriterInnerDelayMs(10)
                .setReaderCount(5)
                .setReaderInnerDelayMs(50)
                .setReaderBatchSize(40)
                .setDurationSec(300)
                .setLongTxEnabled(true)
                .setHost(host)
                .setPort(port)
                .build();

        StressTestRunner.run(config);
    }

    @Test
    void test_PgQueueBuffer_4_SelectForUpdateSkipLockedWhereIdSyncCommitOff() {
        StressTestConfig config = StressTestConfig.builder()
                .setPgQueueBuffer(new PgQueueBuffer_4_SelectForUpdateSkipLockedWhereIdSyncCommitOff(10))
                .setWriterCount(70)
                .setWriterInnerDelayMs(10)
                .setReaderCount(5)
                .setReaderInnerDelayMs(50)
                .setReaderBatchSize(40)
                .setDurationSec(300)
                .setLongTxEnabled(true)
                .setHost(host)
                .setPort(port)
                .build();

        StressTestRunner.run(config);
    }

    @Test
    void test_PgQueueBuffer_5_SelectForUpdateSkipLockedWhereIdSyncCommitOffAndTruncate() {
        StressTestConfig config = StressTestConfig.builder()
                .setPgQueueBuffer(new PgQueueBuffer_5_SelectForUpdateSkipLockedWhereIdSyncCommitOffAndTruncate(
                        10, 3, 10))
                .setWriterCount(70)
                .setWriterInnerDelayMs(10)
                .setReaderCount(5)
                .setReaderInnerDelayMs(50)
                .setReaderBatchSize(40)
                .setDurationSec(300)
                .setLongTxEnabled(true)
                .setHost(host)
                .setPort(port)
                .build();

        StressTestRunner.run(config);
    }
}
