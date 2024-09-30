package io.milvus.v2.common;

public enum BulkInsertState {
    ImportPending(0),              // the task in in pending list of rootCoord, waiting to be executed
    ImportFailed(1),               // the task failed for some reason, get detail reason from GetImportStateResponse.infos
    ImportStarted(2),              // the task has been sent to datanode to execute
    ImportPersisted(5),            // all data files have been parsed and all meta data already persisted, ready to be flushed.
    ImportCompleted(6),            // all indexes are successfully built and segments are able to be compacted as normal.
    ImportFailedAndCleaned(7),     // the task failed and all segments it generated are cleaned up.
    ImportFlushed(8);              // all segments are successfully flushed.

    private final int code;

    BulkInsertState(int i) {
        this.code = i;
    }
}
