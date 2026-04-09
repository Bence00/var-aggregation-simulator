package com.example.risk.aggregation.streaming;

import com.example.risk.model.RiskRecord;

/**
 * Ordered source of {@link RiskRecord} instances.
 *
 * <p>The caller guarantees records arrive already ordered by the interesting
 * dimensions required by prefix-rollup processing.
 */
@FunctionalInterface
public interface OrderedRiskRecordSource {

    void forEach(RecordConsumer consumer) throws Exception;

    @FunctionalInterface
    interface RecordConsumer {
        void accept(RiskRecord record) throws Exception;
    }
}
