package io.milvus.param;

public enum IndexBuildState {
    IndexStateNone,
    Unissued,
    InProgress,
    Finished,
    Failed,
    Retry,
}
