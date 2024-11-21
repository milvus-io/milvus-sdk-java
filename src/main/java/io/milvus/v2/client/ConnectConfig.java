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

package io.milvus.v2.client;

import static io.milvus.common.constant.MilvusClientConstant.MilvusConsts.CLOUD_SERVERLESS_URI_REGEX;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.StringUtils;

import javax.net.ssl.SSLContext;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

@Data
@SuperBuilder
public class ConnectConfig {
    @NonNull
    private String uri;
    private String token;
    private String username;
    private String password;
    private String dbName;
    @Builder.Default
    private long connectTimeoutMs = 10000;
    @Builder.Default
    private long keepAliveTimeMs = 55000;
    @Builder.Default
    private long keepAliveTimeoutMs = 20000;
    @Builder.Default
    private boolean keepAliveWithoutCalls = false;
    @Builder.Default
    private long rpcDeadlineMs = 0; // Disabling deadline

    private String clientKeyPath;
    private String clientPemPath;
    private String caPemPath;
    private String serverPemPath;
    private String serverName;
    @Builder.Default
    private Boolean secure = false;
    @Builder.Default
    private long idleTimeoutMs = TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS);

    private SSLContext sslContext;

    public String getHost() {
        io.milvus.utils.URLParser urlParser = new io.milvus.utils.URLParser(this.uri);
        return urlParser.getHostname();
    }

    public int getPort() {
        io.milvus.utils.URLParser urlParser = new io.milvus.utils.URLParser(this.uri);
        int port = urlParser.getPort();
        if (Pattern.matches(CLOUD_SERVERLESS_URI_REGEX, this.uri)) {
            port = 443;
        }
        return port;
    }

    public String getAuthorization() {
        if (token != null) {
            return token;
        }else if (username != null && password != null) {
            return username + ":" + password;
        }
        return null;
    }

    public String getDbName() {
        io.milvus.utils.URLParser urlParser = new io.milvus.utils.URLParser(this.uri);
        return StringUtils.isNotEmpty(urlParser.getDatabase()) ? urlParser.getDatabase() : this.dbName;
    }

    public Boolean isSecure() {
        if(uri.startsWith("https")) {
            return true;
        }
        return secure;
    }
}
