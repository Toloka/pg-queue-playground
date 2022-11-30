package ai.toloka.engineering.pg_queue_playground.misc;

import java.sql.ResultSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LongTransactionKeeper implements Runnable {

    private static final Logger logger = LogManager.getLogger();

    private final TransactionManager txManager;

    private volatile boolean enabled = true;

    public LongTransactionKeeper(TransactionManager txManager) {
        this.txManager = txManager;
    }

    @Override
    public void run() {
        txManager.begin();
        txManager.execute("select txid_current()", ResultSet::next);
        logger.info("Long transaction is started");
        while (enabled) {
            Util.sleep(100);
        }
        txManager.commit();
        logger.info("Long transaction is stopped");
    }

    public void stop() {
        enabled = false;
    }
}
