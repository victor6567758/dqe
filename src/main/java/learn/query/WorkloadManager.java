package learn.query;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Workload manager: admits queries into a queue, limits how many run at once,
 * and cancels queries that run too long.
 */
public class WorkloadManager implements AutoCloseable {

    /** Settings for one queue. */
    public record QueueConfig(String name, int maxConcurrency, long jobMemoryBytes,
                              Duration enqueueTimeout, Duration runtimeLimit) {
        public QueueConfig {
            if (maxConcurrency < 1) {
                throw new IllegalArgumentException("maxConcurrency must be at least 1");
            }
        }
    }

    /** Thrown when a query waits too long for a free slot in its queue. */
    public static class QueryRejectedException extends RuntimeException {
        public QueryRejectedException(String message) {
            super(message);
        }
    }

    /** Thrown when a query runs longer than the queue's runtime limit. */
    public static class QueryCancelledException extends RuntimeException {
        public QueryCancelledException(String message) {
            super(message);
        }
    }

    private final QueueConfig config;
    private final Semaphore slots;
    private final ExecutorService runner = Executors.newCachedThreadPool();

    public WorkloadManager(QueueConfig config) {
        this.config = config;
        this.slots = new Semaphore(config.maxConcurrency(), true);
    }

    /** Runs one query through the queue. */
    public <T> T run(Callable<T> query) throws Exception {
        if (!slots.tryAcquire(config.enqueueTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
            throw new QueryRejectedException("Queue '" + config.name() + "' was full for "
                + config.enqueueTimeout());
        }
        try {
            Future<T> future = runner.submit(query);
            try {
                return future.get(config.runtimeLimit().toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                future.cancel(true);
                throw new QueryCancelledException("Query exceeded runtime limit of " + config.runtimeLimit());
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof Exception ex) {
                    throw ex;
                }
                throw new IllegalStateException(cause);
            }
        } finally {
            slots.release();
        }
    }

    public QueueConfig config() {
        return config;
    }

    @Override
    public void close() {
        runner.shutdownNow();
    }
}
