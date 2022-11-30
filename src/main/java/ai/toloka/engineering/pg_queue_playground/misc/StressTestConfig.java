package ai.toloka.engineering.pg_queue_playground.misc;

import ai.toloka.engineering.pg_queue_playground.PgQueueBuffer;
import ai.toloka.engineering.pg_queue_playground.PgQueueBuffer_1_SelectForUpdate;

public final class StressTestConfig {

    public final PgQueueBuffer buffer;
    public final int writerCount;
    public final int writerInnerDelayMs;
    public final int readerCount;
    public final int readerInnerDelayMs;
    public final int readerBatchSize;
    public final int durationMs;
    public final boolean longTxEnabled;

    public final String host;
    public final int port;
    public final String db;
    public final String username;
    public final String password;
    public final int maxPoolSize;

    // see pg-configs/sync-replica/postgresql.conf (recovery_min_apply_delay)
    public final int syncReplicaDelayMs = 50;

    public StressTestConfig(PgQueueBuffer buffer, int writerCount, int writerInnerDelayMs, int readerCount,
                            int readerInnerDelayMs, int readerBatchSize, int durationSec, boolean longTxEnabled,
                            String host, int port, String db, String username, String password, int maxPoolSize) {
        this.buffer = buffer;
        this.writerCount = writerCount;
        this.writerInnerDelayMs = writerInnerDelayMs;
        this.readerCount = readerCount;
        this.readerInnerDelayMs = readerInnerDelayMs;
        this.readerBatchSize = readerBatchSize;
        this.durationMs = durationSec * 1_000;
        this.longTxEnabled = longTxEnabled;

        this.host = host;
        this.port = port;
        this.db = db;
        this.username = username;
        this.password = password;
        this.maxPoolSize = maxPoolSize;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "StressTestConfig{" +
                "buffer=" + buffer +
                ", writerCount=" + writerCount +
                ", writerInnerDelayMs=" + writerInnerDelayMs +
                ", readerCount=" + readerCount +
                ", readerInnerDelayMs=" + readerInnerDelayMs +
                ", readerBatchSize=" + readerBatchSize +
                ", durationMs=" + durationMs +
                ", longTxEnabled=" + longTxEnabled +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", db='" + db + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", maxPoolSize=" + maxPoolSize +
                ", syncReplicaDelayMs=" + syncReplicaDelayMs +
                '}';
    }

    @SuppressWarnings("unused")
    public static final class Builder {

        private PgQueueBuffer buffer = new PgQueueBuffer_1_SelectForUpdate();
        private int writerCount = 70;
        private int writerInnerDelayMs = 10;
        private int readerCount = 5;
        private int readerInnerDelayMs = 50;
        private int readerBatchSize = 40;
        private int durationSec = 300;
        private boolean longTxEnabled = true;

        private String host = "localhost";
        private int port = 5432; // see pg-configs/primary/postgresql.conf (port)
        private String db = "pgqb_db"; // see docker-compose.yml (PG_DATABASE)
        private String username = "pgqb_user"; // see docker-compose.yml (PG_USER)
        private String password = "pgqb_password"; // see docker-compose.yml (PG_PASSWORD)
        private int maxPoolSize = 500; // see pg-configs/primary/postgresql.conf (max_connections)

        public StressTestConfig build() {
            return new StressTestConfig(buffer, writerCount, writerInnerDelayMs, readerCount, readerInnerDelayMs,
                    readerBatchSize, durationSec, longTxEnabled, host, port, db, username, password, maxPoolSize);
        }

        public Builder setPgQueueBuffer(PgQueueBuffer buffer) {
            this.buffer = buffer;
            return this;
        }

        public Builder setWriterCount(int writerCount) {
            this.writerCount = writerCount;
            return this;
        }

        public Builder setWriterInnerDelayMs(int writerInnerDelayMs) {
            this.writerInnerDelayMs = writerInnerDelayMs;
            return this;
        }

        public Builder setReaderCount(int readerCount) {
            this.readerCount = readerCount;
            return this;
        }

        public Builder setReaderInnerDelayMs(int readerInnerDelayMs) {
            this.readerInnerDelayMs = readerInnerDelayMs;
            return this;
        }

        public Builder setReaderBatchSize(int readerBatchSize) {
            this.readerBatchSize = readerBatchSize;
            return this;
        }

        public Builder setDurationSec(int durationSec) {
            this.durationSec = durationSec;
            return this;
        }

        public Builder setLongTxEnabled(boolean longTxEnabled) {
            this.longTxEnabled = longTxEnabled;
            return this;
        }

        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public Builder setPort(Integer port) {
            this.port = port;
            return this;
        }

        public Builder setDb(String db) {
            this.db = db;
            return this;
        }

        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setMaxPoolSize(int maxPoolSize) {
            this.maxPoolSize = maxPoolSize;
            return this;
        }
    }
}
