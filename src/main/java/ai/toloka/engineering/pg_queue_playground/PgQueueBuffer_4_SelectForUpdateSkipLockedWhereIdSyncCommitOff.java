package ai.toloka.engineering.pg_queue_playground;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import ai.toloka.engineering.pg_queue_playground.misc.TransactionManager;

public class PgQueueBuffer_4_SelectForUpdateSkipLockedWhereIdSyncCommitOff extends AbstractPgQueueBuffer {

    private final int resetEvery;

    private final AtomicLong resetCount = new AtomicLong();

    private volatile long lastId = -1;

    public PgQueueBuffer_4_SelectForUpdateSkipLockedWhereIdSyncCommitOff(int resetEvery) {
        super(false);
        this.resetEvery = resetEvery;
    }

    @Override
    public void pollInner(int count, Consumer<List<Event>> consumer) {
        if (resetCount.incrementAndGet() % resetEvery == 0) {
            lastId = -1;
        }

        List<Event> events = new ArrayList<>();
        String sql = "select id, payload " +
                "from queue_buffer " +
                "where id>? " +
                "order by id " +
                "for update skip locked " +
                "limit ?";
        txManager.execute(sql, ps -> {
            ps.setLong(1, lastId);
            ps.setInt(2, count);
        }, rs -> {
            while (rs.next()) {
                long id = rs.getLong(1);
                String payload = rs.getString(2);
                events.add(new Event(id, payload));
            }
        });

        if (events.isEmpty()) {
            return;
        }

        consumer.accept(events);

        String params = events.stream().map(e -> "?").collect(Collectors.joining(","));
        sql = "delete from queue_buffer where id in (" + params + ")";
        txManager.execute(sql, (TransactionManager.PreparedStatementConsumer) ps -> {
            for (int i = 0; i < events.size(); i++) {
                ps.setLong(i + 1, events.get(i).id);
            }
        });

        txManager.execute("set local synchronous_commit to off");

        lastId = events.get(events.size() - 1).id;
    }
}
