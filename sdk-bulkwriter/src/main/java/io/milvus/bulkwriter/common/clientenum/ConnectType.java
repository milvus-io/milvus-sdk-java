package io.milvus.bulkwriter.common.clientenum;

/**
 * The connection type used by the Milvus BulkWriter to reach the remote storage and Milvus endpoints.
 */
public enum ConnectType {
    /** Automatically select between internal and public connections. */
    AUTO,
    /** Use the internal connection, typically within the same private network as the service. */
    INTERNAL,
    /** Use the public connection over the internet. */
    PUBLIC
}
