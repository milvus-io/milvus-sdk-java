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

package io.milvus.utils;

import lombok.Getter;
import lombok.ToString;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

/**
 * @author: wei.hu@zilliz.com
 */
@Getter
@ToString
public class URLParser {

    private String hostname;
    private int port;
    private String database;
    private boolean secure;

    public URLParser(String url) {
        try {
            // secure
            if (url.startsWith("https://")) {
                secure = true;
            }

            // host
            URI uri = new URI(url);
            hostname = uri.getHost();
            if (Objects.isNull(hostname)) {
                throw new IllegalArgumentException("Missing hostname in url");
            }

            // port
            port = uri.getPort();
            if(port <= 0){
                port = 19530;
            }

            // database
            String path = uri.getPath();
            if (Objects.isNull(path) || path.isEmpty() || "/".equals(path)) {
                database = null;
            } else {
                database = path.substring(1);
            }
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid url: " + url, e);
        }
    }

}