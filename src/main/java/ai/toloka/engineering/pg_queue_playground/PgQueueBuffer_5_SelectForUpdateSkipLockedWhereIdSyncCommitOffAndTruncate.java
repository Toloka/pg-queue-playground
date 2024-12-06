package ai.toloka.engineering.pg_queue_playground;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import ai.toloka.engineering.pg_queue_playground.misc.TransactionManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static ai.toloka.engineering.pg_queue_playground.misc.Util.elapsedMs;
import static ai.toloka.engineering.pg_queue_playground.misc.Util.isElapsed;

public class PgQueueBuffer_5_SelectForUpdateSkipLockedWhereIdSyncCommitOffAndTruncate
        extends AbstractPgQueueBuffer
        implements Runnable, Closeable {

    public static final int MAX_PARTITION_COUNT = 3;
    public static final int TRY_LOCK_TIMEOUT_MS = 10_000;

    private static final Logger logger = LogManager.getLogger();

    private final int resetEvery;
    private final int partitionCount;
    private final int pseudoVacuumDelaySec;
    private final Map<Integer, Long> tableLocks = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicLong> resetCounts = new ConcurrentHashMap<>();
    private final Map<Integer, Long> lastIds = new ConcurrentHashMap<>();

    private volatile boolean pseudoVacuumEnabled = true;

    public PgQueueBuffer_5_SelectForUpdateSkipLockedWhereIdSyncCommitOffAndTruncate(int resetEvery,
                                                                                    int partitionCount,
                                                                                    int pseudoVacuumDelaySec) {
        super(false);

        if (partitionCount > MAX_PARTITION_COUNT) {
            throw new IllegalArgumentException("Partition count should be less or equal to " + MAX_PARTITION_COUNT);
        }

        this.resetEvery = resetEvery;
        this.partitionCount = partitionCount;
        this.pseudoVacuumDelaySec = pseudoVacuumDelaySec;

        for (int i = 1; i <= partitionCount; i++) {
            tableLocks.put(i, 1_000_000_000L + i);
            resetCounts.put(i, new AtomicLong());
            lastIds.put(i, -1L);
        }
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(pseudoVacuumDelaySec * 1_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (!pseudoVacuumEnabled) {
                return;
            }
            runPseudoVacuum();
        }
    }

    @Override
    public void close() {
        pseudoVacuumEnabled = false;
    }

    public void runPseudoVacuum() {
        List<Integer> tableKeys = tableLocks.keySet().stream().sorted().toList();
        for (Integer tableKey : tableKeys) {
            try {
                runVacuumAnalyze(tableKey);
                long startNano = System.nanoTime();
                runPseudoVacuum(tableKey);
                logger.info("Finish pseudo vacuum for the table {} (elapsed {} ms)", tableKey,
                        elapsedMs(startNano));
            } catch (Exception e) {
                logger.error("Unexpected error during pseudo vacuum for the table {}", tableKey, e);
            }
        }
    }

    private void runVacuumAnalyze(Integer tableKey) {
        txManager.executeWithoutTx("vacuum analyze queue_buffer_" + tableKey);
    }

    private void runPseudoVacuum(Integer tableKey) {
        txManager.begin();
        String sql = "SET LOCAL lock_timeout = '30s';" +
                "SET LOCAL statement_timeout = '30s';" +
                "SELECT pg_advisory_xact_lock(?);" +
                "LOCK TABLE queue_buffer_" + tableKey + " IN ACCESS EXCLUSIVE MODE; " +
                "CREATE TEMPORARY TABLE queue_buffer_" + tableKey + "_copy ON COMMIT DROP AS SELECT * FROM " +
                "queue_buffer_" + tableKey + " WITH DATA; " +
                "TRUNCATE queue_buffer_" + tableKey + "; " +
                "INSERT INTO queue_buffer_" + tableKey + " SELECT * FROM queue_buffer_" + tableKey + "_copy";
        txManager.execute(sql, (TransactionManager.PreparedStatementConsumer) ps ->
                ps.setLong(1, tableLocks.get(tableKey)));
        txManager.commit();
    }

    @Override
    public void offer(Event event) {
        int tableKey = getSharedLockOnAnyTable();
        String sql = "insert into queue_buffer_" + tableKey + " (payload) values (?)";
        txManager.execute(sql, (TransactionManager.PreparedStatementConsumer) ps ->
                ps.setString(1, event.payload));
    }

    private int getSharedLockOnAnyTable() {
        List<Integer> shuffledTableKeys = getShuffledTableKeys();
        return tryAnyAdvisoryXactLockShared(shuffledTableKeys);
    }

    private List<Integer> getShuffledTableKeys() {
        List<Integer> keys = new ArrayList<>();
        for (int i = 1; i <= partitionCount; i++) {
            keys.add(i);
        }
        Collections.shuffle(keys);
        return keys;
    }

    private Integer tryAnyAdvisoryXactLockShared(List<Integer> tableKeys) {
        long startNano = System.nanoTime();
        while (!isElapsed(TRY_LOCK_TIMEOUT_MS, startNano)) {
            for (Integer tableKey : tableKeys) {
                if (tryAdvisoryXactLockShared(tableKey)) {
                    return tableKey;
                }
            }
        }
        throw new IllegalStateException("Failed to get a lock for a queue buffer table in "
                + TRY_LOCK_TIMEOUT_MS + " ms");
    }

    private boolean tryAdvisoryXactLockShared(Integer tableKey) {
        boolean[] result = new boolean[1];
        txManager.execute("select pg_try_advisory_xact_lock_shared(?)",
                ps -> ps.setLong(1, tableLocks.get(tableKey)),
                rs -> {
                    rs.next();
                    result[0] = rs.getBoolean(1);
                });
        return result[0];
    }

    @Override
    public void pollInner(int count, Consumer<List<Event>> consumer) {
        int tableKey = getSharedLockOnAnyTable();

        if (resetCounts.get(tableKey).incrementAndGet() % resetEvery == 0) {
            lastIds.put(tableKey, -1L);
        }

        List<Event> events = new ArrayList<>();
        String sql = "select id, payload " +
                "from queue_buffer_" + tableKey + " " +
                "where id>? " +
                "order by id " +
                "for update skip locked " +
                "limit ?";
        txManager.execute(sql, ps -> {
            ps.setLong(1, lastIds.get(tableKey));
            ps.setInt(2, count);
        }, rs -> {
            while (rs.next()) {
                long id = rs.getLong(1);
                String payload = rs.getString(2);
                events.add(new Event(id, payload));
            }
        });

        consumer.accept(events);

        if (events.isEmpty()) {
            return;
        }
        String params = events.stream().map(e -> "?").collect(Collectors.joining(","));
        sql = "delete from queue_buffer_" + tableKey + " where id in (" + params + ")";
        txManager.execute(sql, (TransactionManager.PreparedStatementConsumer) ps -> {
            for (int i = 0; i < events.size(); i++) {
                ps.setLong(i + 1, events.get(i).id);
            }
        });

        txManager.execute("set local synchronous_commit to off");

        lastIds.put(tableKey, events.get(events.size() - 1).id);
    }

    @Override
    public long size() {
        long[] result = new long[1];
        for (int i = 1; i <= partitionCount; i++) {
            String sql = "select count(*) from queue_buffer_" + i;
            txManager.executeWithoutTx(sql, rs -> {
                rs.next();
                result[0] += rs.getLong(1);
            });
        }
        return result[0];
    }
}
