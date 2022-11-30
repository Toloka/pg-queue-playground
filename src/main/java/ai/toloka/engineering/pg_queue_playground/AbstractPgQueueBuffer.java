package ai.toloka.engineering.pg_queue_playground;

import java.util.List;
import java.util.function.Consumer;

import ai.toloka.engineering.pg_queue_playground.misc.TransactionManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractPgQueueBuffer implements PgQueueBuffer {

    protected static final Logger logger = LogManager.getLogger();

    protected final boolean syncCommitEnabled;

    protected volatile TransactionManager txManager;

    protected AbstractPgQueueBuffer(boolean syncCommitEnabled) {
        this.syncCommitEnabled = syncCommitEnabled;
    }

    @Override
    public void init(TransactionManager txManager) {
        this.txManager = txManager;
    }

    @Override
    public boolean isSyncCommitEnabled() {
        return syncCommitEnabled;
    }

    @Override
    public void offer(Event event) {
        txManager.execute("insert into queue_buffer (payload) values (?)",
                (TransactionManager.PreparedStatementConsumer) ps -> ps.setString(1, event.payload));
    }

    @Override
    public void poll(int count, Consumer<List<Event>> consumer) {
        try {
            txManager.begin();
            pollInner(count, consumer);
            txManager.commit();
        } catch (RuntimeException e) {
            logger.error("Unexpected exception while polling", e);
            txManager.rollbackSafely();
        }
    }

    protected abstract void pollInner(int count, Consumer<List<Event>> consumer);

    @Override
    public long size() {
        long[] result = new long[1];
        txManager.executeWithoutTx("select count(*) from queue_buffer", rs -> {
            rs.next();
            result[0] = rs.getLong(1);
        });
        return result[0];
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "syncCommitEnabled=" + syncCommitEnabled +
                '}';
    }
}
