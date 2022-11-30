package ai.toloka.engineering.pg_queue_playground.misc;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractReaderWriter implements Runnable {

    protected static final Logger logger = LogManager.getLogger();

    protected final String name;

    private final CyclicBarrier barrier;

    private volatile boolean enabled = true;

    private long deltaSum;
    private long countSum;

    protected AbstractReaderWriter(String name, CyclicBarrier barrier) {
        this.name = name;
        this.barrier = barrier;
    }

    @Override
    public void run() {
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            logger.error("Unexpected exception", e);
            throw new RuntimeException(e);
        }
        while (enabled) {
            try {
                runInner();
            } catch (RuntimeException e) {
                logger.error("Unexpected exception in reader/writer", e);
                break;
            }
        }
    }

    protected abstract void runInner();

    public void stop() {
        enabled = false;
    }

    protected synchronized void logStat(long deltaNanos, int count) {
        deltaSum += deltaNanos;
        countSum += count;
    }

    public synchronized Stat getStatAndReset() {
        Stat stat = new Stat(deltaSum, countSum);
        deltaSum = 0;
        countSum = 0;
        return stat;
    }

    public record Stat(long deltaSum, long countSum) {
    }
}
