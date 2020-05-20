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

package io.milvus.client;

import java.util.Arrays;
import java.util.Optional;

/**
 * Represents response of a client call. Contains a <code>status</code> and a <code>message</code>
 */
public class Response {

  private final Status status;
  private final String message;

  public Response(Status status, String message) {
    this.status = status;
    this.message = message;
  }

  public Response(Status status) {
    this.status = status;
    if (status == Status.CLIENT_NOT_CONNECTED) {
      this.message = "You are not connected to Milvus server";
    } else {
      this.message = "Success!";
    }
  }

  public Status getStatus() {
    return status;
  }

  public String getMessage() {
    return message;
  }

  /** @return <code>true</code> if status equals SUCCESS */
  public boolean ok() {
    return status == Status.SUCCESS;
  }

  @Override
  public String toString() {
    return String.format("Response {code = %s, message = \"%s\"}", status.name(), this.message);
  }

  /** Represents server and client side status code */
  public enum Status {
    // Server side error
    SUCCESS(0),
    UNEXPECTED_ERROR(1),
    CONNECT_FAILED(2),
    PERMISSION_DENIED(3),
    COLLECTION_NOT_EXISTS(4),
    ILLEGAL_ARGUMENT(5),
    ILLEGAL_DIMENSION(7),
    ILLEGAL_INDEX_TYPE(8),
    ILLEGAL_COLLECTION_NAME(9),
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

    // Client side error
    RPC_ERROR(-1),
    CLIENT_NOT_CONNECTED(-2),
    UNKNOWN(-3);

    private final int code;

    Status(int code) {
      this.code = code;
    }

    public static Status valueOf(int val) {
      Optional<Status> search =
          Arrays.stream(values()).filter(status -> status.code == val).findFirst();
      return search.orElse(UNKNOWN);
    }

    public int getCode() {
      return code;
    }
  }
}
