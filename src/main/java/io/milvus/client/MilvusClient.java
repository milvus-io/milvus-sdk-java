package io.milvus.client;

public interface MilvusClient {

    String clientVersion = "0.1.0";

    /**
     * @return the current Milvus client version
     */
    default String clientVersion() {
        return clientVersion;
    }

    /**
     * Connects to Milvus server
     * @param connectParam the <code>ConnectParam</code> object
     *                     <pre>
     *                     example usage:
     *                     <code>
     *                         ConnectParam connectParam = new ConnectParam.Builder()
     *                                                                     .withHost("localhost")
     *                                                                     .withPort("19530")
     *                                                                     .build();
     *                     </code>
     *                     </pre>
     * @return <code>Response</code>
     * @see ConnectParam
     */
    Response connect(ConnectParam connectParam);

    boolean connected();

    Response disconnect() throws InterruptedException;

    Response createTable(TableSchemaParam tableSchemaParam);

    HasTableResponse hasTable(TableParam tableParam);

    Response dropTable(TableParam tableParam);

    Response createIndex(IndexParam indexParam);

    InsertResponse insert(InsertParam insertParam);

    SearchResponse search(SearchParam searchParam);

    SearchResponse searchInFiles(SearchInFilesParam searchInFilesParam);

    DescribeTableResponse describeTable(TableParam tableParam);

    ShowTablesResponse showTables();

    GetTableRowCountResponse getTableRowCount(TableParam tableParam);

    Response serverStatus();

    Response serverVersion();

//    Response command(CommandParam commandParam);

    Response deleteByRange(DeleteByRangeParam deleteByRangeParam);

    Response preloadTable(TableParam tableParam);

    DescribeIndexResponse describeIndex(TableParam tableParam);

    Response dropIndex(TableParam tableParam);
}
