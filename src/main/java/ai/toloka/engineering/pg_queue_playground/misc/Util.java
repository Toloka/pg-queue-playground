package ai.toloka.engineering.pg_queue_playground.misc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Util {

    private static final Logger logger = LogManager.getLogger();

    public static boolean isElapsed(int durationMs, long startNano) {
        long deltaMs = elapsedMs(startNano);
        return deltaMs > durationMs;
    }

    public static long elapsedMs(long startNano) {
        return (System.nanoTime() - startNano) / 1_000_000L;
    }

    public static long timed(Runnable runnable) {
        long from = System.nanoTime();
        runnable.run();
        long to = System.nanoTime();
        return to - from;
    }

    public static void sleep(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            logger.error("Unexpected interrupted exception", e);
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
