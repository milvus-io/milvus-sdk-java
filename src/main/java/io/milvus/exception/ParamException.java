package io.milvus.exception;

public class ParamException extends RuntimeException{
    private String msg;
    private Integer status;

    public ParamException(String msg) {
        this.msg = msg;
        this.status = -1;
    }


    public ParamException(String msg, Integer status) {
        this.msg = msg;
        this.status = status;
    }



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
}
