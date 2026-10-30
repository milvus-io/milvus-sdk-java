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

public enum ErrorCode {
    /**
     * The operation completed successfully.
     */
    SUCCESS(0),
    /**
     * The specified collection could not be found.
     */
    COLLECTION_NOT_FOUND(1),
    /**
     * An error occurred on the Milvus server.
     */
    SERVER_ERROR(2),
    /**
     * The request contained invalid parameters.
     */
    INVALID_PARAMS(3),
    /**
     * An error occurred on the client side.
     */
    CLIENT_ERROR(4),
    /**
     * A gRPC communication error occurred.
     */
    RPC_ERROR(5),
    /**
     * The operation exceeded its timeout.
     */
    TIMEOUT(6);

    private final int code;

    ErrorCode(int i) {
        this.code = i;
    }

    public int getCode() {
        return code;
    }
}
