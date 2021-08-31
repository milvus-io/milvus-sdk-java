package io.milvus.param;

public enum MetricType {
    INVALID,
    L2,
    IP,
    //Only supported for byte vectors
    HAMMING,
    JACCARD,
    TANIMOTO,
    SUBSTRUCTURE,
    SUPERSTRUCTURE,
    ;

}
