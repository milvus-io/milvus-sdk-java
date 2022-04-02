package io.milvus.param;

import io.milvus.exception.ParamException;
import lombok.NonNull;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class ServerAddress {
    private final String host;
    private final int port;
    private final int healthPort;

    private ServerAddress(@NonNull Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.healthPort = builder.healthPort;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getHealthPort() {
        return healthPort;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String host = "localhost";
        private int port = 19530;
        private int healthPort = 9091;

        private Builder() {
        }

        /**
         * Sets the host name/address.
         *
         * @param host host name/address
         * @return <code>Builder</code>
         */
        public Builder withHost(@NonNull String host) {
            this.host = host;
            return this;
        }

        /**
         * Sets the connection port. Port value must be greater than zero and less than 65536.
         *
         * @param port port value
         * @return <code>Builder</code>
         */
        public Builder withPort(int port)  {
            this.port = port;
            return this;
        }

        /**
         * Sets the cluster health port. Port value must be greater than zero and less than 65536.
         *
         * @param port port value
         * @return <code>Builder</code>
         */
        public Builder withHealthPort(int port)  {
            this.healthPort = port;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link ServerAddress} instance.
         *
         * @return {@link ServerAddress}
         */
        public ServerAddress build() throws ParamException {
            ParamUtils.CheckNullEmptyString(host, "Host name");

            if (port < 0 || port > 0xFFFF) {
                throw new ParamException("Port is out of range!");
            }

            if (healthPort < 0 || healthPort > 0xFFFF) {
                throw new ParamException("Health Port is out of range!");
            }

            return new ServerAddress(this);
        }
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("ServerAddress{");
        sb.append("host='").append(host).append('\'');
        sb.append(", port=").append(port);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        ServerAddress that = (ServerAddress) o;

        return new EqualsBuilder()
                .append(port, that.port)
                .append(host, that.host)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(host)
                .append(port)
                .toHashCode();
    }
}
