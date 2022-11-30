package ai.toloka.engineering.pg_queue_playground;

import java.util.List;
import java.util.function.Consumer;

import ai.toloka.engineering.pg_queue_playground.misc.TransactionManager;

public interface PgQueueBuffer {

    void init(TransactionManager txManager);

    void offer(Event event);

    void poll(int count, Consumer<List<Event>> consumer);

    long size();

    boolean isSyncCommitEnabled(); // just for overhead reporting purpose
}
