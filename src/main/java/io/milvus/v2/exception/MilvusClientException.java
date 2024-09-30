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

import lombok.Getter;

@Getter
public class MilvusClientException extends RuntimeException {

    private final ErrorCode errorCode;

    private int serverErrCode = 0;
    private int legacyServerCode = 0;

    public MilvusClientException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public MilvusClientException(ErrorCode errorCode, Throwable e) {
        super(e);
        this.errorCode = errorCode;
    }

    public MilvusClientException(ErrorCode errorCode, String message, int serverErrCode, int legacyServerCode) {
        super(message);
        this.errorCode = errorCode;
        this.serverErrCode = serverErrCode;
        this.legacyServerCode = legacyServerCode;
    }
}
