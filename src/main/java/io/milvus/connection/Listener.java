package io.milvus.connection;

/**
 * Interface of multi server listener.
 */
public interface Listener {

    Boolean heartBeat(ServerSetting serverSetting);

}
