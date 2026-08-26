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

package io.milvus.common.utils;

import io.milvus.exception.UnExpectedException;
import io.milvus.param.R;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for throwing and handling exceptions in the Milvus SDK.
 *
 * <p>These helpers centralize error logging, response-status checking and argument validation so
 * that the callers report consistent error messages.
 */
public class ExceptionUtils {
    private static final Logger logger = LoggerFactory.getLogger(ExceptionUtils.class);

    /**
     * Logs the given message at error level and throws an {@link UnExpectedException}.
     *
     * @param msg the error message
     * @throws UnExpectedException always
     */
    public static void throwUnExpectedException(String msg) {
        logger.error(msg);
        throw new UnExpectedException(msg);
    }

    /**
     * Checks the status of a Milvus response wrapper and throws a {@link RuntimeException} carrying
     * the returned error message when the status is not {@code Success}.
     *
     * @param r the response wrapper to check
     * @throws RuntimeException if the response status is not {@code Success}
     */
    public static void handleResponseStatus(R<?> r) {
        if (r.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException(r.getMessage());
        }
    }

    /**
     * Verifies that the given object is not {@code null}.
     *
     * @param obj the object to check
     * @param msg the message prefix describing the missing argument
     * @throws IllegalArgumentException if {@code obj} is {@code null}
     */
    public static void checkNotNull(Object obj, String msg) {
        if (obj == null) {
            throw new IllegalArgumentException(msg + "cannot be null");
        }
    }
}
