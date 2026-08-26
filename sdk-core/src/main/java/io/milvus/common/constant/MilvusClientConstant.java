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

/**
 * Constants used across the Milvus SDK client, including URI prefixes and commonly used string
 * values.
 */
public class MilvusClientConstant {

    /**
     * Constants related to the Milvus connection endpoint.
     */
    public static class MilvusConsts {
        /**
         * The {@code "https://"} scheme prefix of a URI.
         */
        public final static String HOST_HTTPS_PREFIX = "https://";

        /**
         * The {@code "http://"} scheme prefix of a URI.
         */
        public final static String HOST_HTTP_PREFIX = "http://";

        /**
         * Regular expression matching Zilliz Cloud serverless endpoints.
         */
        public final static String CLOUD_SERVERLESS_URI_REGEX = "^(https://in03-.{20,}zilliz.*.(com|cn))|(https://in0\\d{1}-.{15,}serverless.*zilliz.*.(com|cn))$";
    }

    /**
     * Commonly used string values.
     */
    public static class StringValue {
        /**
         * A single colon character.
         */
        public final static String COLON = ":";
        /**
         * A double slash.
         */
        public final static String DOUBLE_SLASH = "//";
    }

}
