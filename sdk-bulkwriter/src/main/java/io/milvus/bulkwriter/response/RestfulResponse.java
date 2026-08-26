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

/**
 * A generic REST response envelope returned by the Milvus cloud REST APIs.
 *
 * <p>It carries a business code, a descriptive message, and the response payload data.</p>
 */
public class RestfulResponse<T> implements Serializable {
    private static final long serialVersionUID = -7162743560382861611L;
    private int code;
    private String message;
    private T data;

    /**
     * Constructs an empty {@code RestfulResponse}.
     */
    public RestfulResponse() {
    }

    /**
     * Constructs a {@code RestfulResponse} with the given code, message, and data.
     *
     * @param code    the business code returned by the server
     * @param message the descriptive message returned by the server
     * @param data    the response payload data
     */
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

    /**
     * Returns the business code returned by the server.
     *
     * @return the business code
     */
    public int getCode() {
        return code;
    }

    /**
     * Sets the business code returned by the server.
     *
     * @param code the business code
     */
    public void setCode(int code) {
        this.code = code;
    }

    /**
     * Returns the descriptive message returned by the server.
     *
     * @return the descriptive message
     */
    public String getMessage() {
        return message;
    }

    /**
     * Sets the descriptive message returned by the server.
     *
     * @param message the descriptive message
     */
    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * Returns the response payload data.
     *
     * @return the response payload data
     */
    public T getData() {
        return data;
    }

    /**
     * Sets the response payload data.
     *
     * @param data the response payload data
     */
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

    /**
     * Returns a new builder for a {@link RestfulResponse}.
     *
     * @return a {@code RestfulResponse} builder
     */
    public static RestfulResponseBuilder<?> builder() {
        return new RestfulResponseBuilder<>();
    }

    /**
     * Builder for {@link RestfulResponse}.
     */
    public static class RestfulResponseBuilder<T> {
        private int code;
        private String message;
        private T data;

        private RestfulResponseBuilder() {
            this.code = 0;
            this.message = "";
            this.data = null;
        }

        /**
         * Sets the business code.
         *
         * @param code the business code
         * @return this builder
         */
        public RestfulResponseBuilder<T> code(int code) {
            this.code = code;
            return this;
        }

        /**
         * Sets the descriptive message.
         *
         * @param message the descriptive message
         * @return this builder
         */
        public RestfulResponseBuilder<T> message(String message) {
            this.message = message;
            return this;
        }

        /**
         * Sets the response payload data.
         *
         * @param data the response payload data
         * @return this builder
         */
        public RestfulResponseBuilder<T> data(T data) {
            this.data = data;
            return this;
        }

        /**
         * Builds the {@link RestfulResponse} instance.
         *
         * @return the built {@code RestfulResponse}
         */
        public RestfulResponse<T> build() {
            return new RestfulResponse<>(this);
        }
    }
}
