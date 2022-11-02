package io.milvus.response;

import io.milvus.exception.IllegalResponseException;
import io.milvus.grpc.ImportResponse;
import lombok.NonNull;

/**
 * Util class to wrap response of <code>bulkInsert</code> interface.
 */
public class BulkInsertResponseWrapper {
    private final ImportResponse response;

    public BulkInsertResponseWrapper(@NonNull ImportResponse response) {
        this.response = response;
    }

    /**
     * Gets ID of the bulk insert task.
     *
     * @return Long ID of the bulk insert task
     */
    public long getTaskID() {
        if (response.getTasksCount() == 0) {
            throw new IllegalResponseException("no task id returned from server");
        }
        return response.getTasks(0);
    }

    /**
     * Construct a <code>String</code> by {@link BulkInsertResponseWrapper} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "bulk insert task state{" +
                ", taskId:" + getTaskID() +
                '}';
    }
}
