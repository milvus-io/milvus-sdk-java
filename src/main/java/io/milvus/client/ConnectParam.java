package io.milvus.client;

import javax.annotation.Nonnull;

public class ConnectParam {
    private final String host;
    private final String port;

    public static class Builder {
        // Optional parameters - initialized to default values
        private String host = "127.0.0.1";
        private String port = "19530";

        public Builder withHost(String host) {
            this.host = host;
            return this;
        }

        public Builder withPort(String port) {
            this.port = port;
            return this;
        }

        public ConnectParam build() {
            return new ConnectParam(this);
        }
    }

    private ConnectParam(@Nonnull Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
    }

    public String getHost() {
        return host;
    }

    public String getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "ConnectParam {" +
                "host='" + host + '\'' +
                ", port='" + port + '\'' +
                '}';
    }
}
