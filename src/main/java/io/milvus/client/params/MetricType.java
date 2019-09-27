package io.milvus.client.params;

public enum MetricType {
    L2(1),
    IP(2);

    private final int metricType;

    MetricType(int metricType) {
        this.metricType = metricType;
    }

    public int getVal() {
        return metricType;
    }
}
