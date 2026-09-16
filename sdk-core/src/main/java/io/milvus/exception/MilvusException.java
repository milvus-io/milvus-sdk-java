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

package io.milvus.exception;

/**
 * Base class of Milvus exceptions.
 */


public class MilvusException extends RuntimeException {
    protected Integer status;

    /**
     * Creates a Milvus exception with a message and status code.
     *
     * @param msg    the error message
     * @param status the Milvus status code
     */


    public MilvusException(String msg, Integer status) {
        super(msg);
        this.status = status;
    }

    /**
     * Returns the Milvus status code of this exception.
     *
     * @return the status code
     */


    public Integer getStatus() {
        return status;
    }

    /**
     * Sets the Milvus status code of this exception.
     *
     * @param status the status code to set
     */


    public void setStatus(Integer status) {
        this.status = status;
    }
}
