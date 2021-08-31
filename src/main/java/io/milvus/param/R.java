package io.milvus.param;

import java.util.Arrays;
import java.util.Optional;

public class R<T> {
    private Exception exception;
    private Integer status;
    private T data;

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public static <T> R<T> failed(Exception exception) {
        R<T> r = new R<>();
        r.setStatus(Status.Unknown.getCode());
        r.setException(exception);
        return r;
    }

    public static <T> R<T> failed(Status statusCode){
        R<T> r = new R<>();
        r.setStatus(statusCode.getCode());
        r.setException(new Exception(statusCode.name()));
        return r;
    }

    public static <T> R<T> success() {
        R<T> r = new R<>();
        r.setStatus(Status.Success.getCode());
        return r;
    }


    public static <T> R<T> success(T data) {
        R<T> r = new R<>();
        r.setStatus(Status.Success.getCode());
        r.setData(data);
        return r;
    }

    @Override
    public String toString() {
        return String.format("status:%s,\n" +
                "data:%s", status, data.toString());
    }
    /** Represents server and client side status code */
    public enum Status {
        // Server side error
        Success(0),
        UnexpectedError(1),
        ConnectFailed(2),
        PermissionDenied(3),
        CollectionNotExists(4),
        IllegalArgument(5),
        IllegalDimension(7),
        IllegalIndexType(8),
        IllegalCollectionName(9),
        IllegalTOPK(10),
        IllegalRowRecord(11),
        IllegalVectorID(12),
        IllegalSearchResult(13),
        FileNotFound(14),
        MetaFailed(15),
        CacheFailed(16),
        CannotCreateFolder(17),
        CannotCreateFile(18),
        CannotDeleteFolder(19),
        CannotDeleteFile(20),
        BuildIndexError(21),
        IllegalNLIST(22),
        IllegalMetricType(23),
        OutOfMemory(24),
        IndexNotExist(25),
        EmptyCollection(26),

        // internal error code.
        DDRequestRace(1000),

        // Client side error
        RpcError(-1),
        ClientNotConnected(-2),
        Unknown(-3),
        VersionMismatch(-4);

        private final int code;

        Status(int code) {
            this.code = code;
        }

        public static Status valueOf(int val) {
            Optional<Status> search =
                    Arrays.stream(values()).filter(status -> status.code == val).findFirst();
            return search.orElse(Unknown);
        }

        public int getCode() {
            return code;
        }
    }
}
