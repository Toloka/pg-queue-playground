package ai.toloka.engineering.pg_queue_playground.misc;

import java.util.concurrent.CyclicBarrier;

import ai.toloka.engineering.pg_queue_playground.PgQueueBuffer;

public class Reader extends AbstractReaderWriter {

    private static int counter = 1;

    private final PgQueueBuffer buffer;

    private final int innerDelayMs;
    private final int batchSize;
    private final int syncReplicaDelayMs;

    public Reader(PgQueueBuffer buffer, CyclicBarrier barrier,
                  int innerDelayMs, int batchSize,
                  boolean syncCommitEnabled, int syncReplicaDelayMs) {
        super("reader-" + counter, barrier);

        this.buffer = buffer;

        this.innerDelayMs = innerDelayMs;
        this.batchSize = batchSize;
        this.syncReplicaDelayMs = syncCommitEnabled ? syncReplicaDelayMs : 0;

        counter += 1;
    }

    @Override
    protected void runInner() {
        int[] count = new int[1];
        long deltaNanos = Util.timed(() -> buffer.poll(batchSize, events -> {
            count[0] = events.size();
            Util.sleep(innerDelayMs);
        }));
        if (count[0] > 0) {
            logStat(deltaNanos - (innerDelayMs + syncReplicaDelayMs) * 1_000_000L, count[0]);
        }
    }
}
