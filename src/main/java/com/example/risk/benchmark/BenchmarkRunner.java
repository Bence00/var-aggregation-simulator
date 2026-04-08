package com.example.risk.benchmark;

import java.util.concurrent.Callable;

/**
 * Minimal benchmarking utility. Wraps a task and returns wall-clock ms.
 */
public final class BenchmarkRunner {

    private BenchmarkRunner() {}

    /**
     * Run {@code task} and return elapsed milliseconds.
     *
     * <p>Uses {@link System#nanoTime()} instead of {@code currentTimeMillis()} to avoid
     * the ~15 ms OS-timer quantisation on Windows, which would round short operations to 0 ms.
     */
    public static <T> TimedResult<T> time(Callable<T> task) {
        long start = System.nanoTime();
        try {
            T value = task.call();
            return new TimedResult<>(value, nanosToMs(System.nanoTime() - start), null);
        } catch (Exception e) {
            return new TimedResult<>(null, nanosToMs(System.nanoTime() - start), e);
        }
    }

    private static long nanosToMs(long nanos) {
        return nanos / 1_000_000L;
    }

    /** Overload for void tasks (returns null value). */
    public static TimedResult<Void> time(ThrowingRunnable task) {
        return time(() -> { task.run(); return null; });
    }

    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Exception;
    }

    /** Result wrapper: carries the return value, elapsed time, and any exception. */
    public static final class TimedResult<T> {
        public final T         value;
        public final long      elapsedMs;
        public final Exception error;

        TimedResult(T value, long elapsedMs, Exception error) {
            this.value     = value;
            this.elapsedMs = elapsedMs;
            this.error     = error;
        }

        public boolean isSuccess() { return error == null; }

        /** Rethrow the error as RuntimeException if one occurred. */
        public T unwrap() {
            if (error != null) throw new RuntimeException(error);
            return value;
        }
    }
}
