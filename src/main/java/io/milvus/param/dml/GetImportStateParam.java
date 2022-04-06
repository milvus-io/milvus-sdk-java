package io.milvus.param.dml;

import io.milvus.exception.ParamException;
import lombok.Getter;
import lombok.NonNull;

/**
 * Parameters for <code>getImportState</code> interface.
 */
@Getter
public class GetImportStateParam {
    private final long taskID;

    private GetImportStateParam(@NonNull Builder builder) {
        this.taskID = builder.taskID;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link GetImportStateParam} class.
     */
    public static class Builder {
        private Long taskID;

        private Builder() {
        }

        /**
         * Sets an import task id. The id is returned from importData() interface.
         *
         * @param taskID id of the task
         * @return <code>Builder</code>
         */
        public Builder withTaskID(@NonNull Long taskID) {
            this.taskID = taskID;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link GetImportStateParam} instance.
         *
         * @return {@link GetImportStateParam}
         */
        public GetImportStateParam build() throws ParamException {
            if (this.taskID == null) {
                throw new ParamException("Task ID not specified");
            }

            return new GetImportStateParam(this);
        }
    }

    /**
     * Constructs a <code>String</code> by {@link GetImportStateParam} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "GetImportStateParam{" +
                "taskID='" + taskID + '\'' +
                '}';
    }
}
