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

package io.milvus.unit.common.utils;

import io.milvus.common.utils.ExceptionUtils;
import io.milvus.exception.UnExpectedException;
import io.milvus.param.R;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
class ExceptionUtilsTest {

    @Test
    void throwUnExpectedExceptionThrowsWithMessage() {
        UnExpectedException ex = Assertions.assertThrows(UnExpectedException.class,
                () -> ExceptionUtils.throwUnExpectedException("boom"));
        Assertions.assertEquals("boom", ex.getMessage());
        Assertions.assertEquals(R.Status.UnexpectedError.getCode(), ex.getStatus());
    }

    @Test
    void handleResponseStatusAcceptsSuccessResponse() {
        ExceptionUtils.handleResponseStatus(R.success());
    }

    @Test
    void handleResponseStatusRejectsFailureResponse() {
        R<String> failed = R.failed(R.Status.ParamError, "bad request");
        RuntimeException ex = Assertions.assertThrows(RuntimeException.class,
                () -> ExceptionUtils.handleResponseStatus(failed));
        Assertions.assertEquals("bad request", ex.getMessage());
    }

    @Test
    void handleResponseStatusCarriesMessageFromFailureResponse() {
        R<String> failed = R.failed(new Exception("server exploded"));
        RuntimeException ex = Assertions.assertThrows(RuntimeException.class,
                () -> ExceptionUtils.handleResponseStatus(failed));
        Assertions.assertEquals("server exploded", ex.getMessage());
    }

    @Test
    void checkNotNullAcceptsNonNullObject() {
        ExceptionUtils.checkNotNull("value", "arg");
    }

    @Test
    void checkNotNullRejectsNullWithMessageSuffix() {
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> ExceptionUtils.checkNotNull(null, "arg"));
        Assertions.assertEquals("argcannot be null", ex.getMessage());
    }
}
