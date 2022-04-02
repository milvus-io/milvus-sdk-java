package io.milvus.connection;

import io.milvus.client.MilvusClient;
import io.milvus.exception.ParamException;
import io.milvus.param.ConnectParam;
import io.milvus.param.ParamUtils;
import io.milvus.param.ServerAddress;
import lombok.NonNull;

/**
 * Defined address and Milvus clients for each server.
 */
public class ServerSetting {
    private final ServerAddress serverAddress;
    private final MilvusClient client;

    public ServerSetting(@NonNull Builder builder) {
        this.serverAddress = builder.serverAddress;
        this.client = builder.milvusClient;
    }

    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    public MilvusClient getClient() {
        return client;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private ServerAddress serverAddress;
        private MilvusClient milvusClient;

        private Builder() {
        }


        /**
         * Sets the server address
         *
         * @param serverAddress ServerAddress host,port/server
         * @return <code>Builder</code>
         */
        public Builder withHost(@NonNull ServerAddress serverAddress) {
            this.serverAddress = serverAddress;
            return this;
        }

        /**
         * Sets the server client for a cluster
         *
         * @param milvusClient MilvusClient
         * @return <code>Builder</code>
         */
        public Builder withMilvusClient(MilvusClient milvusClient) {
            this.milvusClient = milvusClient;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link ConnectParam} instance.
         *
         * @return {@link ConnectParam}
         */
        public ServerSetting build() throws ParamException {
            ParamUtils.CheckNullEmptyString(serverAddress.getHost(), "Host name");

            if (serverAddress.getPort() < 0 || serverAddress.getPort() > 0xFFFF) {
                throw new ParamException("Port is out of range!");
            }

            if (milvusClient == null) {
                throw new ParamException("Milvus client can not be empty");
            }

            return new ServerSetting(this);
        }
    }
}
