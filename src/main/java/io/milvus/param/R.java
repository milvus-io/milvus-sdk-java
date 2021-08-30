package io.milvus.param;

import io.milvus.grpc.ErrorCode;

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
        r.setStatus(-1);
        r.setException(exception);
        return r;
    }

    public static <T> R<T> failed(ErrorCode errorCode) {
        R<T> r = new R<>();
        r.setStatus(errorCode.ordinal());
        r.setException(new Exception(errorCode.name()));
        return r;
    }

    public static <T> R<T> success() {
        R<T> r = new R<>();
        r.setStatus(0);
        return r;
    }


    public static <T> R<T> success(T data) {
        R<T> r = new R<>();
        r.setStatus(0);
        r.setData(data);
        return r;
    }

    @Override
    public String toString() {
        return String.format("status:%s,data:{}", status, data.toString());
    }
}
