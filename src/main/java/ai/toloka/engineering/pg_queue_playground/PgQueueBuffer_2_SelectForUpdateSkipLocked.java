package ai.toloka.engineering.pg_queue_playground;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import ai.toloka.engineering.pg_queue_playground.misc.TransactionManager;

public class PgQueueBuffer_2_SelectForUpdateSkipLocked extends AbstractPgQueueBuffer {

    public PgQueueBuffer_2_SelectForUpdateSkipLocked() {
        super(true);
    }

    @Override
    public void pollInner(int count, Consumer<List<Event>> consumer) {
        List<Event> events = new ArrayList<>();
        String sql = "select id, payload " +
                "from queue_buffer " +
                "order by id " +
                "for update skip locked " +
                "limit ?";
        txManager.execute(sql,
                ps -> ps.setInt(1, count),
                rs -> {
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
    }
}
