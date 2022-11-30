package ai.toloka.engineering.pg_queue_playground.misc;

import java.util.UUID;
import java.util.concurrent.CyclicBarrier;

import ai.toloka.engineering.pg_queue_playground.Event;
import ai.toloka.engineering.pg_queue_playground.PgQueueBuffer;

public class Writer extends AbstractReaderWriter {

    private static int counter = 1;

    private final PgQueueBuffer buffer;
    private final TransactionManager txManager;

    private final int innerDelayMs;
    private final int syncReplicaDelayMs;

    public Writer(PgQueueBuffer buffer, TransactionManager txManager, CyclicBarrier barrier, int innerDelayMs,
                  int syncReplicaDelayMs) {
        super("writer-" + counter, barrier);

        this.buffer = buffer;
        this.txManager = txManager;

        this.innerDelayMs = innerDelayMs;
        this.syncReplicaDelayMs = syncReplicaDelayMs;

        counter += 1;
    }

    @Override
    protected void runInner() {
        long deltaNanos = Util.timed(() -> {
            txManager.begin();
            Util.sleep(innerDelayMs);
            Event event = new Event("payload_" + UUID.randomUUID());
            buffer.offer(event);
            txManager.commit(); // stuck at least on syncReplicaDelayMs
        });
        logStat(deltaNanos - (innerDelayMs + syncReplicaDelayMs) * 1_000_000L, 1);
    }
}
