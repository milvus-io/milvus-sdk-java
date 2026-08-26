package io.milvus.param;

/**
 * Represents the build states of an index.
 */
public enum IndexBuildState {
    IndexStateNone,
    Unissued,
    InProgress,
    Finished,
    Failed,
    Retry,
}
