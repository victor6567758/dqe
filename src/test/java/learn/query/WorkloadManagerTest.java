package learn.query;

import learn.query.WorkloadManager.QueryCancelledException;
import learn.query.WorkloadManager.QueryRejectedException;
import learn.query.WorkloadManager.QueueConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

class WorkloadManagerTest {

    private static final long MB = 1024L * 1024;

    @Test
    void returnsTheResultOfAQueryWithinLimits() throws Exception {
        QueueConfig config = new QueueConfig("test", 2, 64 * MB, Duration.ofSeconds(1), Duration.ofSeconds(5));
        try (WorkloadManager workloads = new WorkloadManager(config)) {
            assertEquals(42, workloads.run(() -> 42));
        }
    }

    @Test
    void propagatesFailuresFromTheQuery() throws Exception {
        QueueConfig config = new QueueConfig("test", 2, 64 * MB, Duration.ofSeconds(1), Duration.ofSeconds(5));
        try (WorkloadManager workloads = new WorkloadManager(config)) {
            assertThrows(IllegalStateException.class,
                () -> workloads.run(() -> { throw new IllegalStateException("boom"); }));
        }
    }

    @Test
    void rejectsQueryThatWaitsTooLongForASlot() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        QueueConfig config = new QueueConfig("test", 1, 64 * MB, Duration.ofMillis(50), Duration.ofSeconds(5));

        try (WorkloadManager workloads = new WorkloadManager(config)) {
            Thread holder = new Thread(() -> {
                try {
                    workloads.run(() -> {
                        started.countDown();
                        release.await();
                        return 1;
                    });
                } catch (Exception ignored) {
                    // the holder's result does not matter for this test
                }
            });
            holder.start();
            try {
                started.await();
                assertThrows(QueryRejectedException.class, () -> workloads.run(() -> 2));
            } finally {
                release.countDown();
                holder.join();
            }
        }
    }

    @Test
    void cancelsQueryThatExceedsRuntimeLimit() throws Exception {
        QueueConfig config = new QueueConfig("test", 1, 64 * MB, Duration.ofSeconds(1), Duration.ofMillis(100));
        try (WorkloadManager workloads = new WorkloadManager(config)) {
            assertThrows(QueryCancelledException.class, () -> workloads.run(() -> {
                Thread.sleep(5_000);
                return 1;
            }));
        }
    }

    @Test
    void rejectsQueueWithNoSlots() {
        assertThrows(IllegalArgumentException.class,
            () -> new QueueConfig("bad", 0, 64 * MB, Duration.ofSeconds(1), Duration.ofSeconds(1)));
    }
}
