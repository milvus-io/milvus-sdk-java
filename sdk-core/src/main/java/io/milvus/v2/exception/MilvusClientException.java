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

package io.milvus.v2.exception;

/**
 * Runtime exception thrown by the Milvus Java SDK v2 when a client or server-side error occurs.
 *
 * <p>The exception carries an {@link ErrorCode} describing the failure category, and optionally
 * the raw error codes returned by the Milvus server.</p>
 */
public class MilvusClientException extends RuntimeException {

    private final ErrorCode errorCode;

    private int serverErrCode = 0;
    private int legacyServerCode = 0;

    /**
     * Constructs a {@code MilvusClientException} with the given error code and message.
     *
     * @param errorCode the error code of this exception
     * @param message   the detail message
     */
    public MilvusClientException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    /**
     * Constructs a {@code MilvusClientException} with the given error code and cause.
     *
     * @param errorCode the error code of this exception
     * @param e         the underlying cause of this exception
     */
    public MilvusClientException(ErrorCode errorCode, Throwable e) {
        super(e);
        this.errorCode = errorCode;
    }

    /**
     * Constructs a {@code MilvusClientException} with the given error code, message, and server
     * error codes.
     *
     * @param errorCode       the error code of this exception
     * @param message         the detail message
     * @param serverErrCode   the error code returned by the Milvus server
     * @param legacyServerCode the legacy error code returned by older Milvus server versions
     */
    public MilvusClientException(ErrorCode errorCode, String message, int serverErrCode, int legacyServerCode) {
        super(message);
        this.errorCode = errorCode;
        this.serverErrCode = serverErrCode;
        this.legacyServerCode = legacyServerCode;
    }

    // Getters
    /**
     * Returns the error code of this exception.
     *
     * @return the error code of this exception
     */
    public ErrorCode getErrorCode() {
        return errorCode;
    }

    /**
     * Returns the error code returned by the Milvus server, or {@code 0} if not set.
     *
     * @return the server error code
     */
    public int getServerErrCode() {
        return serverErrCode;
    }

    /**
     * Returns the legacy error code returned by older Milvus server versions, or {@code 0} if not
     * set.
     *
     * @return the legacy server error code
     */
    public int getLegacyServerCode() {
        return legacyServerCode;
    }

    @Override
    public String toString() {
        String s = super.toString();
        String codeMsg = " ErrorCode: " + errorCode.name();
        if (serverErrCode > 0) {
            codeMsg += (", ServerCode: " + serverErrCode);
        } else if (legacyServerCode > 0) {
            codeMsg += (", ServerCode: " + legacyServerCode);
        }
        return s + codeMsg;
    }
}
