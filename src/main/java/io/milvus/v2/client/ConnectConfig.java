package io.milvus.v2.client;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.util.concurrent.TimeUnit;

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

    public String getHost() {
        URI uri = URI.create(this.uri);
        return uri.getHost();
    }

    public int getPort() {
        URI uri = URI.create(this.uri);
        return uri.getPort();
    }

    public String getAuthorization() {
        if (token != null) {
            return token;
        }else if (username != null && password != null) {
            return username + ":" + password;
        }
        return null;
    }

    public Boolean isSecure() {
        if(uri.startsWith("https")) {
            return true;
        }
        return secure;
    }
}
