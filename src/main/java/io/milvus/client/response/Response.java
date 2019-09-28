package io.milvus.client.response;

import java.util.Arrays;
import java.util.Optional;

public class Response {

    public enum Status {
        SUCCESS(0),
        UNEXPECTED_ERROR(1),
        CONNECT_FAILED(2),
        PERMISSION_DENIED(3),
        TABLE_NOT_EXISTS(4),
        ILLEGAL_ARGUMENT(5),
        ILLEGAL_RANGE(6),
        ILLEGAL_DIMENSION(7),
        ILLEGAL_INDEX_TYPE(8),
        ILLEGAL_TABLE_NAME(9),
        ILLEGAL_TOPK(10),
        ILLEGAL_ROWRECORD(11),
        ILLEGAL_VECTOR_ID(12),
        ILLEGAL_SEARCH_RESULT(13),
        FILE_NOT_FOUND(14),
        META_FAILED(15),
        CACHE_FAILED(16),
        CANNOT_CREATE_FOLDER(17),
        CANNOT_CREATE_FILE(18),
        CANNOT_DELETE_FOLDER(19),
        CANNOT_DELETE_FILE(20),
        BUILD_INDEX_ERROR(21),
        ILLEGAL_NLIST(22),
        ILLEGAL_METRIC_TYPE(23),
        OUT_OF_MEMORY(24),

        RPC_ERROR(-1),
        UNKNOWN(-2);

        private final int code;

        Status(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        //TODO: Potential performance issue?
        public static Status valueOf(int val) {
            Optional<Status> search = Arrays.stream(values())
                                            .filter(status -> status.code == val)
                                            .findFirst();
            return search.orElse(UNKNOWN);
        }
    };

    private final Status status;
    private final String message;

    public Response(Status status, String message) {
        this.status = status;
        this.message = message;
    }

    public Response(Status status) {
        this.status = status;
        this.message = "Success!";
    }

    public Status getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }

    public boolean ok() {
        return status == Status.SUCCESS;
    }

    @Override
    public String toString() {
        return String.format("Response {code = %s, message = %s}", status.name(), this.message);
    }
}
