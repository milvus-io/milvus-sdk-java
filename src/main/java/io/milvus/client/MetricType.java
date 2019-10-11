package io.milvus.client;

import java.util.Arrays;
import java.util.Optional;

/**
 * Represents available metric types
 */
public enum MetricType {
    L2(1),
    IP(2),

    UNKNOWN(-1);

    private final int val;

    MetricType(int val) {
        this.val = val;
    }

    public int getVal() {
        return val;
    }

    public static MetricType valueOf(int val) {
        Optional<MetricType> search = Arrays.stream(values())
                                     .filter(metricType -> metricType.val == val)
                                     .findFirst();
        return search.orElse(UNKNOWN);
    }
}
