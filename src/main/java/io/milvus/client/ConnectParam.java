package io.milvus.client;

import javax.annotation.Nonnull;

/**
 * Contains parameters for connecting to Milvus server
 */
public class ConnectParam {
    private final String host;
    private final String port;

    /**
     * Builder for <code>ConnectParam</code>
     */
    public static class Builder {
        // Optional parameters - initialized to default values
        private String host = "127.0.0.1";
        private String port = "19530";

        /**
         * Optional. Default to "127.0.0.1".
         * @param host server host
         * @return <code>Builder</code>
         */
        public Builder withHost(@Nonnull String host) {
            this.host = host;
            return this;
        }

        /**
         * Optional. Default to "19530".
         * @param port server port
         * @return <code>Builder</code>
         */
        public Builder withPort(@Nonnull String port) {
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
