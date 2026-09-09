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

package io.milvus.unit.common.constant;

import io.milvus.common.constant.MilvusClientConstant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

@Tag("unit")
class MilvusClientConstantTest {

    @Test
    void uriPrefixConstantsExistWithExpectedValues() {
        Assertions.assertEquals("https://", MilvusClientConstant.MilvusConsts.HOST_HTTPS_PREFIX);
        Assertions.assertEquals("http://", MilvusClientConstant.MilvusConsts.HOST_HTTP_PREFIX);
    }

    @Test
    void stringValueConstantsExistWithExpectedValues() {
        Assertions.assertEquals(":", MilvusClientConstant.StringValue.COLON);
        Assertions.assertEquals("//", MilvusClientConstant.StringValue.DOUBLE_SLASH);
    }

    @Test
    void serverlessRegexMatchesZillizCloudEndpoints() {
        Assertions.assertTrue(Pattern.matches(
                MilvusClientConstant.MilvusConsts.CLOUD_SERVERLESS_URI_REGEX,
                "https://in03-abcdefghijklmnopqrst.zilliz.com"));
        Assertions.assertTrue(Pattern.matches(
                MilvusClientConstant.MilvusConsts.CLOUD_SERVERLESS_URI_REGEX,
                "https://in02-abcdefghijklmnop.serverless.aws.zilliz.com"));
        Assertions.assertTrue(Pattern.matches(
                MilvusClientConstant.MilvusConsts.CLOUD_SERVERLESS_URI_REGEX,
                "https://in01-abcdefghijklmnopq.serverless-zilliz.cn"));
    }

    @Test
    void serverlessRegexRejectsOtherEndpoints() {
        Assertions.assertFalse(Pattern.matches(
                MilvusClientConstant.MilvusConsts.CLOUD_SERVERLESS_URI_REGEX,
                "http://in03-abcdefghijklmnopqrst.zilliz.com"));
        Assertions.assertFalse(Pattern.matches(
                MilvusClientConstant.MilvusConsts.CLOUD_SERVERLESS_URI_REGEX,
                "https://in03-abc.zilliz.com"));
        Assertions.assertFalse(Pattern.matches(
                MilvusClientConstant.MilvusConsts.CLOUD_SERVERLESS_URI_REGEX,
                "https://milvus.example.com:19530"));
    }
}
