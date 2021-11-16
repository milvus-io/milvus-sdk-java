/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.milvus.param;

import io.milvus.exception.MilvusException;
import io.milvus.grpc.ErrorCode;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.Arrays;
import java.util.Optional;

/**
 * Util class to wrap gpc response and exceptions.
 */
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

    public String getMessage() { return exception.getMessage(); }

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

    /**
     * Wrap an exception for failure.
     *
     * @param exception exception object
     * @return <code>R</code>
     */
    public static <T> R<T> failed(Exception exception) {
        R<T> r = new R<>();
        if (exception instanceof MilvusException) {
            MilvusException e = (MilvusException) exception;
            r.setStatus(e.getStatus());
        } else {
            r.setStatus(Status.Unknown.getCode());
            r.exception = exception;
        }
        r.setException(exception);
        return r;
    }

    /**
     * Wrap an error code and error message for failure.
     *
     * @param errorCode rpc error code
     * @param msg error message
     * @return <code>R</code>
     */
    public static <T> R<T> failed(ErrorCode errorCode, String msg) {
        R<T> r = new R<>();
        r.setStatus(errorCode.ordinal());
        r.setException(new Exception(msg));
        return r;
    }

    /**
     * Wrap a status code and error message for failure.
     *
     * @param statusCode status code
     * @param msg error message
     * @return <code>R</code>
     */
    public static <T> R<T> failed(Status statusCode, String msg) {
        R<T> r = new R<>();
        r.setStatus(statusCode.getCode());
        r.setException(new Exception(msg));
        return r;
    }

    /**
     * Direct return a succeed status.
     *
     * @return <code>R</code>
     */
    public static <T> R<T> success() {
        R<T> r = new R<>();
        r.setStatus(Status.Success.getCode());
        return r;
    }

    /**
     * Wrap a succeed rpc response object.
     *
     * @param data rpc response object
     * @return <code>R</code>
     */
    public static <T> R<T> success(T data) {
        R<T> r = new R<>();
        r.setStatus(Status.Success.getCode());
        r.setData(data);
        return r;
    }

    /**
     * Represents server and client side status code
     */
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
        VersionMismatch(-4),
        ParamError(-5),
        IllegalResponse(-6);

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

    /**
     * Construct a <code>String</code> by <code>R</code> instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        if (exception != null) {
            return "R{" +
                    "exception=" + ExceptionUtils.getMessage(exception) +
                    ", status=" + status +
                    ", data=" + data +
                    '}';
        } else {
            return "R{" +
                    "status=" + status +
                    ", data=" + data +
                    '}';
        }
    }
}
