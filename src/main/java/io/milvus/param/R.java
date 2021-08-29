package io.milvus.param;

import io.milvus.grpc.ErrorCode;

public class R<T> {
    private String msg;
    private Integer status;
    private T data;

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
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

    public static <T> R<T> failed(String msg){
        R<T> r = new R<>();
        r.setStatus(-1);
        r.setMsg(msg);
        return r;
    }

    public static <T> R<T> failed(ErrorCode errorCode){
        R<T> r = new R<>();
        r.setStatus(errorCode.ordinal());
        r.setMsg(errorCode.name());
        return r;
    }

    public static <T> R<T> success(String msg){
        R<T> r = new R<>();
        r.setStatus(0);
        r.setMsg(msg);
        return r;
    }


    public static <T> R<T> success(T data){
        R<T> r = new R<>();
        r.setStatus(0);
        r.setMsg("success");
        r.setData(data);
        return r;
    }

    @Override
    public String toString() {
        return "R{" +
                "msg='" + msg + '\'' +
                ", status=" + status +
                ", data=" + data +
                '}';
    }
}
