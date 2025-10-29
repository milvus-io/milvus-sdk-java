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

package io.milvus.bulkwriter.response;

import java.io.Serializable;

public class RestfulResponse<T> implements Serializable {
    private static final long serialVersionUID = -7162743560382861611L;
    private int code;
    private String message;
    private T data;

    public RestfulResponse() {
    }

    public RestfulResponse(int code, String message, T data) {
        this.code = code;
        this.message = message;
        this.data = data;
    }

    private RestfulResponse(RestfulResponseBuilder<T> builder) {
        this.code = builder.code;
        this.message = builder.message;
        this.data = builder.data;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "RestfulResponse{" +
                "code=" + code +
                ", message='" + message + '\'' +
                '}';
    }

    public static RestfulResponseBuilder<?> builder() {
        return new RestfulResponseBuilder<>();
    }

    public static class RestfulResponseBuilder<T> {
        private int code;
        private String message;
        private T data;

        private RestfulResponseBuilder() {
            this.code = 0;
            this.message = "";
            this.data = null;
        }

        public RestfulResponseBuilder<T> code(int code) {
            this.code = code;
            return this;
        }

        public RestfulResponseBuilder<T> message(String message) {
            this.message = message;
            return this;
        }

        public RestfulResponseBuilder<T> data(T data) {
            this.data = data;
            return this;
        }

        public RestfulResponse<T> build() {
            return new RestfulResponse<>(this);
        }
    }
}
