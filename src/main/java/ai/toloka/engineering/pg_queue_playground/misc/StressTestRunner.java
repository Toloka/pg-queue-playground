package ai.toloka.engineering.pg_queue_playground.misc;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import ai.toloka.engineering.pg_queue_playground.PgQueueBuffer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.Flyway;

public class StressTestRunner {

    private static final Logger logger = LogManager.getLogger();

    public static void run(StressTestConfig config) {
        logger.info("Stress test is started {}", config);
        try (HikariDataSource dataSource = createDataSource(config)) {
            logger.info("Data source is created {}", dataSource);
            runInner(dataSource, config);
            logger.info("Stress test is finished");
        }
    }

    private static HikariDataSource createDataSource(StressTestConfig config) {
        var dsConfig = new HikariConfig();
        dsConfig.setDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");
        dsConfig.setUsername(config.username);
        dsConfig.setPassword(config.password);
        dsConfig.addDataSourceProperty("serverName", config.host);
        dsConfig.addDataSourceProperty("portNumber", config.port);
        dsConfig.addDataSourceProperty("databaseName", config.db);
        dsConfig.setMaximumPoolSize(config.maxPoolSize);
        return new HikariDataSource(dsConfig);
    }

    private static void runInner(HikariDataSource dataSource, StressTestConfig config) {
        var txManager = new TransactionManager(dataSource);

        applyDbMigrations(dataSource);
        logger.info("DB migrations are applied");
        truncateTables(txManager);
        logger.info("All tables are truncated");

        PgQueueBuffer buffer = config.buffer;
        buffer.init(txManager);
        logger.info("Buffer is initialized");

        List<AbstractReaderWriter> readersAndWriters = new ArrayList<>();
        readersAndWriters.addAll(createReaders(buffer, txManager, config));
        readersAndWriters.addAll(createWriters(buffer, txManager, config));
        logger.info("Readers and writers are created");

        var longTxKeeper = new LongTransactionKeeper(txManager);
        ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

        try {
            submitRunnableInExecutorService(buffer, readersAndWriters, longTxKeeper, executorService, config);
            reportStatWhileRunning(buffer, readersAndWriters, config);
        } finally {
            stopExecutorService(buffer, readersAndWriters, longTxKeeper, executorService);
        }
    }

    private static void applyDbMigrations(DataSource dataSource) {
        Flyway flyway = Flyway.configure()
                .dataSource(dataSource)
                .load();
        flyway.migrate();
    }

    private static void truncateTables(TransactionManager txManager) {
        txManager.begin();
        // see resources/db/migration/V1__create_queue_buffer_tables.sql
        txManager.execute("TRUNCATE queue_buffer; " +
                "TRUNCATE queue_buffer_1; " +
                "TRUNCATE queue_buffer_2; " +
                "TRUNCATE queue_buffer_3;");
        txManager.commit();
    }

    private static List<Reader> createReaders(PgQueueBuffer buffer,
                                              TransactionManager txManager,
                                              StressTestConfig config) {
        List<Reader> readers = new ArrayList<>();
        var readerBarrier = new CyclicBarrier(config.readerCount);
        boolean syncCommitEnabled = buffer.isSyncCommitEnabled();
        for (int i = 0; i < config.readerCount; i++) {
            var reader = new Reader(
                    buffer,
                    txManager,
                    readerBarrier,
                    config.readerInnerDelayMs,
                    config.readerBatchSize,
                    syncCommitEnabled,
                    config.syncReplicaDelayMs
            );
            readers.add(reader);
        }
        return readers;
    }

    private static List<Writer> createWriters(PgQueueBuffer buffer,
                                              TransactionManager txManager,
                                              StressTestConfig config) {
        List<Writer> writers = new ArrayList<>();
        var writerBarrier = config.writerCount > 0 ? new CyclicBarrier(config.writerCount) : null;
        for (int i = 0; i < config.writerCount; i++) {
            var writer = new Writer(
                    buffer,
                    txManager,
                    writerBarrier,
                    config.writerInnerDelayMs,
                    config.syncReplicaDelayMs
            );
            writers.add(writer);
        }
        return writers;
    }

    private static void submitRunnableInExecutorService(PgQueueBuffer buffer,
                                                        List<AbstractReaderWriter> readersAndWriters,
                                                        LongTransactionKeeper longTxKeeper,
                                                        ExecutorService executorService,
                                                        StressTestConfig config) {
        for (AbstractReaderWriter readerWriter : readersAndWriters) {
            executorService.submit(readerWriter);
        }
        if (config.longTxEnabled) {
            executorService.submit(longTxKeeper);
        }
        if (buffer instanceof Runnable) {
            executorService.submit((Runnable) buffer);
        }
    }

    private static void stopExecutorService(PgQueueBuffer buffer, List<AbstractReaderWriter> readersAndWriters,
                                            LongTransactionKeeper longTxKeeper, ExecutorService executorService) {
        if (buffer instanceof Closeable) {
            try {
                ((Closeable) buffer).close();
            } catch (IOException e) {
                logger.error("Can't close buffer", e);
            }
        }
        longTxKeeper.stop();
        for (AbstractReaderWriter readerWriter : readersAndWriters) {
            readerWriter.stop();
        }
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                logger.warn("Timeout on termination of the executor");
            }
        } catch (InterruptedException e) {
            logger.error("Unexpected interrupted exception", e);
        }
    }

    private static void reportStatWhileRunning(PgQueueBuffer buffer, List<AbstractReaderWriter> readersAndWriters,
                                               StressTestConfig config) {
        long startNs = System.nanoTime();
        long lastReportNs = startNs;
        do {
            // 998 instead of 1000 is needed to compensate overhead and to adjust reporting at every second
            Util.sleep(998 - Util.elapsedMs(lastReportNs));
            long deltaMs = Util.elapsedMs(lastReportNs);
            lastReportNs = System.nanoTime();
            report(buffer, readersAndWriters, deltaMs);
        } while (!Util.isElapsed(config.durationMs, startNs));
    }

    private static void report(PgQueueBuffer buffer, List<AbstractReaderWriter> readersAndWriters, long deltaMs) {
        ReaderWriterStat readerWriterStat = getStatAndReset(readersAndWriters, deltaMs);
        Stat readerStat = readerWriterStat.readerStat;
        Stat writerStat = readerWriterStat.writerStat;
        logger.info("write throughput {}, read throughput {}, size {} " +
                        "(avg overhead: write {}ms, read {}ms; log rate: write {}, read {})",
                writerStat.count, readerStat.count,
                buffer.size(),
                writerStat.avg, readerStat.avg,
                writerStat.logRate, readerStat.logRate
        );
    }

    private static ReaderWriterStat getStatAndReset(List<AbstractReaderWriter> readerWriters, long deltaMs) {
        long readerDeltaSum = 0;
        long readerCountSum = 0;
        long readerLogCount = 0;
        long readerCount = 0;
        long writerDeltaSum = 0;
        long writerCountSum = 0;
        long writerLogCount = 0;
        long writerCount = 0;
        for (AbstractReaderWriter readerWriter : readerWriters) {
            AbstractReaderWriter.Stat stat = readerWriter.getStatAndReset();
            if (readerWriter instanceof Reader) {
                readerDeltaSum += stat.deltaSum();
                readerCountSum += stat.countSum();
                readerLogCount += stat.logCount();
                readerCount += 1;
            } else {
                writerDeltaSum += stat.deltaSum();
                writerCountSum += stat.countSum();
                writerLogCount += stat.logCount();
                writerCount += 1;
            }
        }
        Stat readerStat;
        Stat writerStat;
        if (readerCountSum == 0) {
            readerStat = new Stat("-1", -1, "-1");
        } else {
            readerStat = new Stat(
                    round(((double) readerDeltaSum) / readerCountSum / 1_000_000, 3),
                    readerCountSum,
                    round(normalizePerSec(((double) readerLogCount) / readerCount, deltaMs), 3)
            );
        }
        if (writerCountSum == 0) {
            writerStat = new Stat("-1", -1, "-1");
        } else {
            writerStat = new Stat(
                    round(((double) writerDeltaSum) / writerCountSum / 1_000_000, 3),
                    writerCountSum,
                    round(normalizePerSec(((double) writerLogCount) / writerCount, deltaMs), 3)
            );
        }
        return new ReaderWriterStat(readerStat, writerStat);
    }

    private static String round(double value, int scale) {
        return BigDecimal.valueOf(value).setScale(scale, RoundingMode.HALF_UP).toPlainString();
    }

    private static double normalizePerSec(double value, long deltaMs) {
        return value * 1_000 / deltaMs;
    }

    private record ReaderWriterStat(Stat readerStat, Stat writerStat) {
    }

    private record Stat(String avg, long count, String logRate) {
    }
}
