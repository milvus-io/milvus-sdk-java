package io.milvus.bulkwriter.connect;

import com.azure.core.credential.TokenCredential;
import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/**
 * Parameters for <code>RemoteBulkWriter</code> interface.
 */
@Getter
@ToString
public class AzureConnectParam extends StorageConnectParam {
    private final String containerName;
    private final String connStr;
    private final String accountUrl;
    private final TokenCredential credential;

    private AzureConnectParam(@NonNull Builder builder) {
        this.containerName = builder.containerName;
        this.connStr = builder.connStr;
        this.accountUrl = builder.accountUrl;
        this.credential = builder.credential;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link AzureConnectParam} class.
     */
    public static final class Builder {
        private String containerName;
        private String connStr;
        private String accountUrl;
        private TokenCredential credential;

        private Builder() {
        }

        /**
         * @param containerName The target container name
         * @return <code>Builder</code>
         */
        public Builder withContainerName(@NonNull String containerName) {
            this.containerName = containerName;
            return this;
        }

        /**
         * @param connStr A connection string to an Azure Storage account,
         *                which can be parsed to an account_url and a credential.
         *                To generate a connection string, read this link:
         *                <a href="https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string">...</a>
         * @return <code>Builder</code>
         */
        public Builder withConnStr(@NonNull String connStr) {
            this.connStr = connStr;
            return this;
        }

        /**
         * @param accountUrl A string in format like "https://[storage-account].blob.core.windows.net"
         *                     Read this link for more info:
         *                     <a href="https://learn.microsoft.com/en-us/azure/storage/common/storage-account-overview">...</a>
         * @return <code>Builder</code>
         */
        public Builder withAccountUrl(@NonNull String accountUrl) {
            this.accountUrl = accountUrl;
            return this;
        }

        /**
         *
         * @param credential Account access key for the account, read this link for more info:
         *                     <a href="https://learn.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal#view-account-access-keys">...</a>
         * @return <code>Builder</code>
         */
        public Builder withCredential(@NonNull TokenCredential credential) {
            this.credential = credential;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link AzureConnectParam} instance.
         *
         * @return {@link AzureConnectParam}
         */
        public AzureConnectParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(containerName, "containerName");

            return new AzureConnectParam(this);
        }
    }
}
