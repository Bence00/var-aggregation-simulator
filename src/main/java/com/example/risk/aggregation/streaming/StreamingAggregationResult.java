package com.example.risk.aggregation.streaming;

/**
 * Timing result for a streaming aggregation + percentile pass.
 */
public final class StreamingAggregationResult {

    private final long orderingMs;
    private final long aggregationMs;
    private final long percentileMs;

    public StreamingAggregationResult(long orderingMs, long aggregationMs, long percentileMs) {
        this.orderingMs = orderingMs;
        this.aggregationMs = aggregationMs;
        this.percentileMs = percentileMs;
    }

    public long orderingMs() {
        return orderingMs;
    }

    public long aggregationMs() {
        return aggregationMs;
    }

    public long percentileMs() {
        return percentileMs;
    }
}
