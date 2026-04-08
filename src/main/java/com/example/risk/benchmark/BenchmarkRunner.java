package com.example.risk.benchmark;

import java.util.concurrent.Callable;

/**
 * Minimal benchmarking utility. Wraps a task and returns wall-clock ms.
 */
public final class BenchmarkRunner {

    private BenchmarkRunner() {}

    /** Run {@code task} and return elapsed milliseconds. */
    public static <T> TimedResult<T> time(Callable<T> task) {
        long start = System.currentTimeMillis();
        try {
            T value = task.call();
            return new TimedResult<>(value, System.currentTimeMillis() - start, null);
        } catch (Exception e) {
            return new TimedResult<>(null, System.currentTimeMillis() - start, e);
        }
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
