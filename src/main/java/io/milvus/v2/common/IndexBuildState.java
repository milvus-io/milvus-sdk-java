package io.milvus.v2.common;

public enum IndexBuildState {
    IndexStateNone,
    Unissued,
    InProgress,
    Finished,
    Failed,
    Retry,
}
