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

package io.milvus.common.constant;

public class MilvusClientConstant {

    public static class MilvusConsts {
        public final static String HOST_HTTPS_PREFIX = "https://";

        public final static String HOST_HTTP_PREFIX = "http://";

        public final static String CLOUD_SERVERLESS_URI_REGEX = "^(https://in03-.{20,}zilliz.*.(com|cn))|(https://in0\\d{1}-.{15,}serverless.*zilliz.*.(com|cn))$";
    }
    public static class StringValue {
        public final static String COLON = ":";
        public final static String DOUBLE_SLASH = "//";
    }

}
