//package io.milvus.param.dml;
//
//import io.milvus.exception.ParamException;
//import lombok.Getter;
//import lombok.NonNull;
//
///**
// * Parameters for <code>getBulkloadState</code> interface.
// */
//@Getter
//public class GetBulkloadStateParam {
//    private final long taskID;
//
//    private GetBulkloadStateParam(@NonNull Builder builder) {
//        this.taskID = builder.taskID;
//    }
//
//    public static Builder newBuilder() {
//        return new Builder();
//    }
//
//    /**
//     * Builder for {@link GetBulkloadStateParam} class.
//     */
//    public static class Builder {
//        private Long taskID;
//
//        private Builder() {
//        }
//
//        /**
//         * Sets an import task id. The id is returned from bulkload() interface.
//         *
//         * @param taskID id of the task
//         * @return <code>Builder</code>
//         */
//        public Builder withTaskID(@NonNull Long taskID) {
//            this.taskID = taskID;
//            return this;
//        }
//
//        /**
//         * Verifies parameters and creates a new {@link GetBulkloadStateParam} instance.
//         *
//         * @return {@link GetBulkloadStateParam}
//         */
//        public GetBulkloadStateParam build() throws ParamException {
//            if (this.taskID == null) {
//                throw new ParamException("Task ID not specified");
//            }
//
//            return new GetBulkloadStateParam(this);
//        }
//    }
//
//    /**
//     * Constructs a <code>String</code> by {@link GetBulkloadStateParam} instance.
//     *
//     * @return <code>String</code>
//     */
//    @Override
//    public String toString() {
//        return "GetBulkloadStateParam{" +
//                "taskID='" + taskID + '\'' +
//                '}';
//    }
//}
