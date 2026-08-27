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

/**
 * Utility methods that mask credentials and other sensitive URI information in log messages.
 */
public final class RedactCredential {
    private static final String REDACTED_CREDENTIAL = "<redacted>";

    private RedactCredential() {
    }

    /**
     * Returns a fixed redacted placeholder for the given credential so that secrets never appear in
     * logs.
     *
     * @param credential the credential value to redact, may be {@code null}
     * @return the redacted placeholder, or {@code null} if the credential is {@code null}
     */
    public static String redactCredential(String credential) {
        return credential == null ? null : REDACTED_CREDENTIAL;
    }

    /**
     * Masks the user-info part of a URI, for example {@code "user:password@host:19530"} becomes
     * {@code "<redacted>@host:19530"}.
     *
     * @param uri the URI to redact, may be {@code null}
     * @return the redacted URI, or {@code null} if the URI is {@code null}
     */
    public static String redactUriUserInfo(String uri) {
        return uri == null ? null : uri.replaceAll(
                "^([^:/?#]+://)?[^/?#]*@", "$1" + REDACTED_CREDENTIAL + "@");
    }
}
