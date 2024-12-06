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
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@TestMethodOrder(MethodOrderer.MethodName.class)
class PgQueueBufferStressTest {

    @SuppressWarnings("resource")
    @ClassRule
    public static DockerComposeContainer<?> compose =
            new DockerComposeContainer<>(new File("docker-compose.yml"))
                    .withExposedService("pg-primary", 5432);

    public static String host = "localhost";
    public static Integer port = 5432;

    @BeforeAll
    public static void init() {
        compose.waitingFor("pg-primary", Wait.forLogMessage(".*START_REPLICATION.*", 1));
        compose.start();
    }

    @Test
    void test_PgQueueBuffer_1_SelectForUpdate() {
        StressTestConfig config = StressTestConfig.builder()
                .setPgQueueBuffer(new PgQueueBuffer_1_SelectForUpdate())
                .setWriterCount(175)
                .setWriterInnerDelayMs(20)
                .setReaderCount(20)
                .setReaderInnerDelayMs(10)
                .setReaderBatchSize(10)
                .setDurationSec(60)
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
                .setWriterCount(175)
                .setWriterInnerDelayMs(20)
                .setReaderCount(20)
                .setReaderInnerDelayMs(10)
                .setReaderBatchSize(10)
                .setDurationSec(60)
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
                .setWriterCount(175)
                .setWriterInnerDelayMs(20)
                .setReaderCount(20)
                .setReaderInnerDelayMs(10)
                .setReaderBatchSize(10)
                .setDurationSec(60)
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
                .setWriterCount(175)
                .setWriterInnerDelayMs(20)
                .setReaderCount(20)
                .setReaderInnerDelayMs(10)
                .setReaderBatchSize(10)
                .setDurationSec(60)
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
                .setWriterCount(175)
                .setWriterInnerDelayMs(20)
                .setReaderCount(20)
                .setReaderInnerDelayMs(10)
                .setReaderBatchSize(10)
                .setDurationSec(60)
                .setLongTxEnabled(true)
                .setHost(host)
                .setPort(port)
                .build();

        StressTestRunner.run(config);
    }
}
