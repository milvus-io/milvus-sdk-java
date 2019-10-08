package io.milvus.client;

import io.grpc.*;

import javax.annotation.Nonnull;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MilvusGrpcClient implements MilvusClient {

    private static final Logger logger = Logger.getLogger(MilvusGrpcClient.class.getName());

    private ManagedChannel channel = null;
    private io.milvus.client.grpc.MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub;

//    private MilvusGrpcClient(String host, int port) {
//        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
//    }
//
//    private MilvusGrpcClient(ManagedChannelBuilder<?> channelBuilder) {
//        channel = channelBuilder.build();
//        blockingStub = io.milvus.client.grpc.MilvusServiceGrpc.newBlockingStub(channel);
//    }

    ///////////////////////Client Calls///////////////////////

    @Override
    public Response connect(ConnectParam connectParam) {
        if (channel != null) {
            logWarning("You have already connected!");
            return new Response(Response.Status.CONNECT_FAILED, "You have already connected!");
        }

        try {
            channel = ManagedChannelBuilder
                    .forAddress(connectParam.getHost(), Integer.parseInt(connectParam.getPort()))
                    .usePlaintext()
                    .build();

            blockingStub = io.milvus.client.grpc.MilvusServiceGrpc.newBlockingStub(channel);
        } catch (Exception e) {
            logSevere("Connect failed!", e.toString());
            return new Response(Response.Status.CONNECT_FAILED);
        }

        logInfo("Connected successfully!\n{0}", connectParam.toString());
        return new Response(Response.Status.SUCCESS);
    }

    @Override
    public boolean connected() {
        return channel != null && !channel.isShutdown() && !channel.isTerminated();
    }

    @Override
    public Response disconnect() throws InterruptedException {
        if (!connected()) {
            logWarning("You are not connected to Milvus server");
            return new Response(Response.Status.CLIENT_NOT_CONNECTED);
        }
        else {
            if (channel.shutdown().awaitTermination(60, TimeUnit.SECONDS)) {
                logInfo("Channel terminated");
            } else {
                logSevere("Encountered error when terminating channel");
                return new Response(Response.Status.RPC_ERROR);
            }
        }
        return new Response(Response.Status.SUCCESS);
    }

    @Override
    public Response createTable(@Nonnull TableSchemaParam tableSchemaParam) {

        if (!connected()) {
            logWarning("You are not connected to Milvus server");
            return new Response(Response.Status.CLIENT_NOT_CONNECTED);
        }

        TableSchema tableSchema = tableSchemaParam.getTableSchema();
        io.milvus.client.grpc.TableSchema request = io.milvus.client.grpc.TableSchema
                                                    .newBuilder()
                                                    .setTableName(tableSchema.getTableName())
                                                    .setDimension(tableSchema.getDimension())
                                                    .setIndexFileSize(tableSchema.getIndexFileSize())
                                                    .setMetricType(tableSchema.getMetricType().getVal())
                                                    .build();

        io.milvus.client.grpc.Status response;

        try {
            response = blockingStub
                       .withDeadlineAfter(tableSchemaParam.getTimeout(), TimeUnit.SECONDS)
                       .createTable(request);

            if (response.getErrorCode() == io.milvus.client.grpc.ErrorCode.SUCCESS) {
                logInfo("Created table successfully!\n{0}", tableSchema.toString());
                return new Response(Response.Status.SUCCESS);
            } else if (response.getReason().contentEquals("Table already exists")) {
                logWarning("Table `{0}` already exists", tableSchema.getTableName());
                return new Response(Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
            } else {
                logSevere("Create table failed\n{0}\n{1}", tableSchema.toString(), response.toString());
                return new Response(Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
            }
        } catch (StatusRuntimeException e) {
            logSevere("createTable RPC failed:\n{0}", e.getStatus().toString());
            return new Response(Response.Status.RPC_ERROR, e.toString());
        }
    }

    @Override
    public HasTableResponse hasTable(@Nonnull TableParam tableParam) {

        if (!connected()) {
            logWarning("You are not connected to Milvus server");
            return new HasTableResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED), false);
        }

        String tableName = tableParam.getTableName();
        io.milvus.client.grpc.TableName request = io.milvus.client.grpc.TableName
                                                  .newBuilder()
                                                  .setTableName(tableName)
                                                  .build();
        io.milvus.client.grpc.BoolReply response;

        try {
            response = blockingStub
                       .withDeadlineAfter(tableParam.getTimeout(), TimeUnit.SECONDS)
                       .hasTable(request);

            if (response.getStatus().getErrorCode() == io.milvus.client.grpc.ErrorCode.SUCCESS) {
                logInfo("hasTable `{0}` = {1}", tableName, response.getBoolReply());
                return new HasTableResponse(new Response(Response.Status.SUCCESS), response.getBoolReply());
            } else {
                logSevere("hasTable `{0}` failed:\n{1}", tableName, response.toString());
                return new HasTableResponse(new Response(Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                                                         response.getStatus().getReason()),
                                    false);
            }
        } catch (StatusRuntimeException e) {
            logSevere("hasTable RPC failed:\n{0}", e.getStatus().toString());
            return new HasTableResponse(new Response(Response.Status.RPC_ERROR, e.toString()), false);
        }
    }

    @Override
    public Response dropTable(@Nonnull TableParam tableParam) {

        if (!connected()) {
            logWarning("You are not connected to Milvus server");
            return new Response(Response.Status.CLIENT_NOT_CONNECTED);
        }

        String tableName = tableParam.getTableName();
        io.milvus.client.grpc.TableName request = io.milvus.client.grpc.TableName
                                                  .newBuilder()
                                                  .setTableName(tableName)
                                                  .build();
        io.milvus.client.grpc.Status response;

        try {
            response = blockingStub
                       .withDeadlineAfter(tableParam.getTimeout(), TimeUnit.SECONDS)
                       .dropTable(request);

            if (response.getErrorCode() == io.milvus.client.grpc.ErrorCode.SUCCESS) {
                logInfo("Dropped table `{0}` successfully!", tableName);
                return new Response(Response.Status.SUCCESS);
            } else {
                logSevere("Drop table `{0}` failed:\n{1}", tableName, response.toString());
                return new Response(Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
            }
        } catch (StatusRuntimeException e) {
            logSevere("dropTable RPC failed:\n{0}", e.getStatus().toString());
            return new Response(Response.Status.RPC_ERROR, e.toString());
        }
    }

    @Override
    public Response createIndex(@Nonnull IndexParam indexParam) {

        if (!connected()) {
            logWarning("You are not connected to Milvus server");
            return new Response(Response.Status.CLIENT_NOT_CONNECTED);
        }

        io.milvus.client.grpc.Index index = io.milvus.client.grpc.Index
                                            .newBuilder()
                                            .setIndexType(indexParam.getIndex().getIndexType().getVal())
                                            .setNlist(indexParam.getIndex().getNList())
                                            .build();
        io.milvus.client.grpc.IndexParam request = io.milvus.client.grpc.IndexParam
                                           .newBuilder()
                                           .setTableName(indexParam.getTableName())
                                           .setIndex(index)
                                           .build();

        io.milvus.client.grpc.Status response;

        try {
            response = blockingStub
                       .withDeadlineAfter(indexParam.getTimeout(), TimeUnit.SECONDS)
                       .createIndex(request);

            if (response.getErrorCode() == io.milvus.client.grpc.ErrorCode.SUCCESS) {
                logInfo("Created index successfully!\n{0}", indexParam.toString());
                return new Response(Response.Status.SUCCESS);
            } else {
                logSevere("Create index failed\n{0}\n{1}", indexParam.toString(), response.toString());
                return new Response(Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
            }
        } catch (StatusRuntimeException e) {
            logSevere("createIndex RPC failed:\n{0}", e.getStatus().toString());
            return new Response(Response.Status.RPC_ERROR, e.toString());
        }
    }

    @Override
    public InsertResponse insert(@Nonnull InsertParam insertParam) {

        if (!connected()) {
            logWarning("You are not connected to Milvus server");
            return new InsertResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED), new ArrayList<>());
        }

        List<io.milvus.client.grpc.RowRecord> rowRecordList = new ArrayList<>();
        for (List<Float> vectors : insertParam.getVectors()) {
            io.milvus.client.grpc.RowRecord rowRecord = io.milvus.client.grpc.RowRecord
                                                        .newBuilder()
                                                        .addAllVectorData(vectors)
                                                        .build();
            rowRecordList.add(rowRecord);
        }

        io.milvus.client.grpc.InsertParam request = io.milvus.client.grpc.InsertParam
                                                    .newBuilder()
                                                    .setTableName(insertParam.getTableName())
                                                    .addAllRowRecordArray(rowRecordList)
                                                    .addAllRowIdArray(insertParam.getVectorIds())
                                                    .build();
        io.milvus.client.grpc.VectorIds response;

        try {
            response = blockingStub
                       .withDeadlineAfter(insertParam.getTimeout(), TimeUnit.SECONDS)
                       .insert(request);

            if (response.getStatus().getErrorCode() == io.milvus.client.grpc.ErrorCode.SUCCESS) {
                Optional<List<Long>> resultVectorIds = Optional.ofNullable(response.getVectorIdArrayList());
                logInfo("Inserted {0} vectors to table `{1} successfully!", resultVectorIds.map(List::size).orElse(0), insertParam.getTableName());
                return new InsertResponse(new Response(Response.Status.SUCCESS), resultVectorIds.orElse(new ArrayList<>()));
            } else {
                logSevere("Insert vectors failed:\n{0}", response.toString());
                return new InsertResponse(new Response(Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                                                       response.getStatus().getReason()),
                                          new ArrayList<>());
            }
        } catch (StatusRuntimeException e) {
            logSevere("insert RPC failed:\n{0}", e.getStatus().toString());
            return new InsertResponse(new Response(Response.Status.RPC_ERROR, e.toString()), new ArrayList<>());
        }
    }

    @Override
    public SearchResponse search(@Nonnull SearchParam searchParam) {

        if (!connected()) {
            logWarning("You are not connected to Milvus server");
            return new SearchResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED), new ArrayList<>());
        }

        List<io.milvus.client.grpc.RowRecord> queryRowRecordList = getQueryRowRecordList(searchParam);

        List<io.milvus.client.grpc.Range> queryRangeList = getQueryRangeList(searchParam);

        io.milvus.client.grpc.SearchParam request = io.milvus.client.grpc.SearchParam
                                                    .newBuilder()
                                                    .setTableName(searchParam.getTableName())
                                                    .addAllQueryRecordArray(queryRowRecordList)
                                                    .addAllQueryRangeArray(queryRangeList)
                                                    .setTopk(searchParam.getTopK())
                                                    .setNprobe(searchParam.getNProbe())
                                                    .build();

        io.milvus.client.grpc.TopKQueryResultList response;

        try {
            response = blockingStub
                       .withDeadlineAfter(searchParam.getTimeout(), TimeUnit.SECONDS)
                       .search(request);

            if (response.getStatus().getErrorCode() == io.milvus.client.grpc.ErrorCode.SUCCESS) {
                List<List<SearchResponse.QueryResult>> queryResultsList = getQueryResultsList(response);
                logInfo("Search completed successfully! Returned results for {0} queries", queryResultsList.size());
                return new SearchResponse(new Response(Response.Status.SUCCESS), queryResultsList);
            } else {
                logSevere("Search failed:\n{0}", response.toString());
                return new SearchResponse(new Response(Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                                                       response.getStatus().getReason()),
                                          new ArrayList<>());
            }
        } catch (StatusRuntimeException e) {
            logSevere("search RPC failed:\n{0}", e.getStatus().toString());
            return new SearchResponse(new Response(Response.Status.RPC_ERROR, e.toString()), new ArrayList<>());
        }
    }

    @Override
    public SearchResponse searchInFiles(@Nonnull SearchInFilesParam searchInFilesParam) {

        if (!connected()) {
            logWarning("You are not connected to Milvus server");
            return new SearchResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED), new ArrayList<>());
        }

        SearchParam searchParam = searchInFilesParam.getSearchParam();

        List<io.milvus.client.grpc.RowRecord> queryRowRecordList = getQueryRowRecordList(searchParam);

        List<io.milvus.client.grpc.Range> queryRangeList = getQueryRangeList(searchParam);

        io.milvus.client.grpc.SearchParam searchParamToSet = io.milvus.client.grpc.SearchParam
                                                             .newBuilder()
                                                             .setTableName(searchParam.getTableName())
                                                             .addAllQueryRecordArray(queryRowRecordList)
                                                             .addAllQueryRangeArray(queryRangeList)
                                                             .setTopk(searchParam.getTopK())
                                                             .setNprobe(searchParam.getNProbe())
                                                             .build();

        io.milvus.client.grpc.SearchInFilesParam request = io.milvus.client.grpc.SearchInFilesParam
                                                           .newBuilder()
                                                           .addAllFileIdArray(searchInFilesParam.getFileIds())
                                                           .setSearchParam(searchParamToSet)
                                                           .build();

        io.milvus.client.grpc.TopKQueryResultList response;

        try {
            response = blockingStub
                       .withDeadlineAfter(searchInFilesParam.getTimeout(), TimeUnit.SECONDS)
                       .searchInFiles(request);

            if (response.getStatus().getErrorCode() == io.milvus.client.grpc.ErrorCode.SUCCESS) {
                logInfo("Search in files {0} completed successfully!", searchInFilesParam.getFileIds());

                List<List<SearchResponse.QueryResult>> queryResultsList = getQueryResultsList(response);
                return new SearchResponse(new Response(Response.Status.SUCCESS), queryResultsList);
            } else {
                logSevere("Search in files {0} failed:\n{1}", searchInFilesParam.getFileIds(), response.toString());
                return new SearchResponse(new Response(Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                                                       response.getStatus().getReason()),
                                          new ArrayList<>());
            }
        } catch (StatusRuntimeException e) {
            logSevere("searchInFiles RPC failed:\n{0}", e.getStatus().toString());
            return new SearchResponse(new Response(Response.Status.RPC_ERROR, e.toString()), new ArrayList<>());
        }
    }

    @Override
    public DescribeTableResponse describeTable(@Nonnull TableParam tableParam) {

        if (!connected()) {
            logWarning("You are not connected to Milvus server");
            return new DescribeTableResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED), null);
        }

        String tableName = tableParam.getTableName();
        io.milvus.client.grpc.TableName request = io.milvus.client.grpc.TableName
                                                  .newBuilder()
                                                  .setTableName(tableName)
                                                  .build();
        io.milvus.client.grpc.TableSchema response;

        try {
            response = blockingStub
                       .withDeadlineAfter(tableParam.getTimeout(), TimeUnit.SECONDS)
                       .describeTable(request);

            if (response.getStatus().getErrorCode() == io.milvus.client.grpc.ErrorCode.SUCCESS) {
                TableSchema tableSchema = new TableSchema.Builder(response.getTableName(), response.getDimension())
                        .withIndexFileSize(response.getIndexFileSize())
                        .withMetricType(MetricType.valueOf(response.getMetricType()))
                        .build();
                logInfo("Describe Table `{0}` returned:\n{1}", tableName, tableSchema);
                return new DescribeTableResponse(new Response(Response.Status.SUCCESS), tableSchema);
            } else {
                logSevere("Describe Table `{0}` failed:\n{1}", tableName, response.toString());
                return new DescribeTableResponse(new Response(Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                                                              response.getStatus().getReason()),
                                      null);
            }
        } catch (StatusRuntimeException e) {
            logSevere("describeTable RPC failed:\n{0}", e.getStatus().toString());
            return new DescribeTableResponse(new Response(Response.Status.RPC_ERROR, e.toString()), null);
        }
    }

    @Override
    public ShowTablesResponse showTables() {

        if (!connected()) {
            logWarning("You are not connected to Milvus server");
            return new ShowTablesResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED), new ArrayList<>());
        }

        io.milvus.client.grpc.Command request = io.milvus.client.grpc.Command
                                                .newBuilder()
                                                .setCmd("")
                                                .build();
        io.milvus.client.grpc.TableNameList response;

        try {
            response = blockingStub
                       .withDeadlineAfter(10, TimeUnit.SECONDS)
                       .showTables(request);

            if (response.getStatus().getErrorCode() == io.milvus.client.grpc.ErrorCode.SUCCESS) {
                List<String> tableNames = response.getTableNamesList();
                logInfo("Current tables: {0}", tableNames.toString());
                return new ShowTablesResponse(new Response(Response.Status.SUCCESS), tableNames);
            } else {
                logSevere("Show tables failed:\n{0}", response.toString());
                return new ShowTablesResponse(new Response(Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                                                           response.getStatus().getReason()),
                                              new ArrayList<>());
            }
        } catch (StatusRuntimeException e) {
            logSevere("showTables RPC failed:\n{0}", e.getStatus().toString());
            return new ShowTablesResponse(new Response(Response.Status.RPC_ERROR, e.toString()), new ArrayList<>());
        }
    }

    @Override
    public GetTableRowCountResponse getTableRowCount(@Nonnull TableParam tableParam) {

        if (!connected()) {
            logWarning("You are not connected to Milvus server");
            return new GetTableRowCountResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED), 0);
        }

        String tableName = tableParam.getTableName();
        io.milvus.client.grpc.TableName request = io.milvus.client.grpc.TableName
                                                  .newBuilder()
                                                  .setTableName(tableName)
                                                  .build();
        io.milvus.client.grpc.TableRowCount response;

        try {
            response = blockingStub
                       .withDeadlineAfter(tableParam.getTimeout(), TimeUnit.SECONDS)
                       .countTable(request);

            if (response.getStatus().getErrorCode() == io.milvus.client.grpc.ErrorCode.SUCCESS) {
                long tableRowCount = response.getTableRowCount();
                logInfo("Table `{0}` has {1} rows", tableName, tableRowCount);
                return new GetTableRowCountResponse(new Response(Response.Status.SUCCESS), tableRowCount);
            } else {
                logSevere("Get table `{0}` row count failed:\n{1}", tableName, response.toString());
                return new GetTableRowCountResponse(new Response(Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                                                                 response.getStatus().getReason()),
                                        0);
            }
        } catch (StatusRuntimeException e) {
            logSevere("countTable RPC failed:\n{0}", e.getStatus().toString());
            return new GetTableRowCountResponse(new Response(Response.Status.RPC_ERROR, e.toString()), 0);
        }
    }

    //Cmd(Command) not implemented

    public Response deleteByRange(@Nonnull DeleteByRangeParam deleteByRangeParam) {

        if (!connected()) {
            logWarning("You are not connected to Milvus server");
            return new Response(Response.Status.CLIENT_NOT_CONNECTED);
        }

        io.milvus.client.grpc.DeleteByRangeParam request = io.milvus.client.grpc.DeleteByRangeParam
                                                           .newBuilder()
                                                           .setRange(getRange(deleteByRangeParam.getDateRange()))
                                                           .setTableName(deleteByRangeParam.getTableName())
                                                           .build();
        io.milvus.client.grpc.Status response;

        try {
            response = blockingStub
                       .withDeadlineAfter(deleteByRangeParam.getTimeout(), TimeUnit.SECONDS)
                       .deleteByRange(request);

            if (response.getErrorCode() == io.milvus.client.grpc.ErrorCode.SUCCESS) {
                logInfo("Deleted vectors from table `{0}` in range {1} successfully!",
                             deleteByRangeParam.getTableName(), deleteByRangeParam.getDateRange().toString());
                return new Response(Response.Status.SUCCESS);
            } else {
                logSevere("Deleted vectors from table `{0}` in range {1} failed:\n{1}", response.toString());
                return new Response(Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
            }
        } catch (StatusRuntimeException e) {
            logSevere("deleteByRange RPC failed:\n{0}", e.getStatus().toString());
            return new Response(Response.Status.RPC_ERROR, e.toString());
        }
    }

    @Override
    public Response preloadTable(@Nonnull TableParam tableParam) {

        if (!connected()) {
            logWarning("You are not connected to Milvus server");
            return new Response(Response.Status.CLIENT_NOT_CONNECTED);
        }

        String tableName = tableParam.getTableName();
        io.milvus.client.grpc.TableName request = io.milvus.client.grpc.TableName
                .newBuilder()
                .setTableName(tableName)
                .build();
        io.milvus.client.grpc.Status response;

        try {
            response = blockingStub
                       .withDeadlineAfter(tableParam.getTimeout(), TimeUnit.SECONDS)
                       .preloadTable(request);

            if (response.getErrorCode() == io.milvus.client.grpc.ErrorCode.SUCCESS) {
                logInfo("Preloaded table `{0}` successfully!", tableName);
                return new Response(Response.Status.SUCCESS);
            } else {
                logSevere("Preload table `{0}` failed:\n{1}", tableName, response.toString());
                return new Response(Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
            }
        } catch (StatusRuntimeException e) {
            logSevere("preloadTable RPC failed:\n{0}", e.getStatus().toString());
            return new Response(Response.Status.RPC_ERROR, e.toString());
        }
    }

    @Override
    public DescribeIndexResponse describeIndex(@Nonnull TableParam tableParam) {

        if (!connected()) {
            logWarning("You are not connected to Milvus server");
            return new DescribeIndexResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED), null);
        }

        String tableName = tableParam.getTableName();
        io.milvus.client.grpc.TableName request = io.milvus.client.grpc.TableName
                                                  .newBuilder()
                                                  .setTableName(tableName)
                                                  .build();
        io.milvus.client.grpc.IndexParam response;

        try {
            response = blockingStub
                       .withDeadlineAfter(tableParam.getTimeout(), TimeUnit.SECONDS)
                       .describeIndex(request);

            if (response.getStatus().getErrorCode() == io.milvus.client.grpc.ErrorCode.SUCCESS) {
                Index index = new Index.Builder()
                        .withIndexType(IndexType.valueOf(response.getIndex().getIndexType()))
                        .withNList(response.getIndex().getNlist())
                        .build();
                IndexParam indexParam = new IndexParam.Builder(response.getTableName())
                        .withIndex(index)
                        .build();
                logInfo("Describe index for table `{0}` returned:\n{1}", tableName, indexParam);
                return new DescribeIndexResponse(new Response(Response.Status.SUCCESS), indexParam);
            } else {
                logSevere("Describe index for table `{0}` failed:\n{1}", tableName, response.toString());
                return new DescribeIndexResponse(new Response(Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                                                              response.getStatus().getReason()),
                                       null);
            }
        } catch (StatusRuntimeException e) {
            logSevere("describeIndex RPC failed:\n{0}", e.getStatus().toString());
            return new DescribeIndexResponse(new Response(Response.Status.RPC_ERROR, e.toString()), null);
        }
    }

    @Override
    public Response dropIndex(@Nonnull TableParam tableParam) {

        if (!connected()) {
            logWarning("You are not connected to Milvus server");
            return new Response(Response.Status.CLIENT_NOT_CONNECTED);
        }

        String tableName = tableParam.getTableName();
        io.milvus.client.grpc.TableName request = io.milvus.client.grpc.TableName
                .newBuilder()
                .setTableName(tableName)
                .build();
        io.milvus.client.grpc.Status response;

        try {
            response = blockingStub.dropIndex(request);

            if (response.getErrorCode() == io.milvus.client.grpc.ErrorCode.SUCCESS) {
                logInfo("Dropped index for table `{0}` successfully!", tableName);
                return new Response(Response.Status.SUCCESS);
            } else {
                logSevere("Drop index for table `{0}` failed:\n{1}", tableName, response.toString());
                return new Response(Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
            }
        } catch (StatusRuntimeException e) {
            logSevere("dropIndex RPC failed:\n{0}", e.getStatus().toString());
            return new Response(Response.Status.RPC_ERROR, e.toString());
        }
    }

    /////////////////////Util Functions/////////////////////
    private List<io.milvus.client.grpc.RowRecord> getQueryRowRecordList(@Nonnull SearchParam searchParam) {
        List<io.milvus.client.grpc.RowRecord> queryRowRecordList = new ArrayList<>();
        for (List<Float> vectors : searchParam.getQueryVectors()) {
            io.milvus.client.grpc.RowRecord rowRecord = io.milvus.client.grpc.RowRecord
                    .newBuilder()
                    .addAllVectorData(vectors)
                    .build();
            queryRowRecordList.add(rowRecord);
        }
        return queryRowRecordList;
    }

    private List<io.milvus.client.grpc.Range> getQueryRangeList(@Nonnull SearchParam searchParam) {
        List<io.milvus.client.grpc.Range> queryRangeList = new ArrayList<>();
        String datePattern = "yyyy-MM-dd";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(datePattern);
        for (DateRange queryRange : searchParam.getQueryRanges()) {
            io.milvus.client.grpc.Range dateRange = io.milvus.client.grpc.Range
                    .newBuilder()
                    .setStartValue(simpleDateFormat.format(queryRange.getStartDate()))
                    .setEndValue(simpleDateFormat.format(queryRange.getEndDate()))
                    .build();
            queryRangeList.add(dateRange);
        }
        return queryRangeList;
    }

    private io.milvus.client.grpc.Range getRange(@Nonnull DateRange dateRange) {
        String datePattern = "yyyy-MM-dd";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(datePattern);
        return io.milvus.client.grpc.Range
                .newBuilder()
                .setStartValue(simpleDateFormat.format(dateRange.getStartDate()))
                .setEndValue(simpleDateFormat.format(dateRange.getEndDate()))
                .build();
    }

    private List<List<SearchResponse.QueryResult>> getQueryResultsList
            (io.milvus.client.grpc.TopKQueryResultList searchResponse) {
        //TODO: refactor
        List<List<SearchResponse.QueryResult>> queryResultsList = new ArrayList<>();
        Optional<List<io.milvus.client.grpc.TopKQueryResult>> topKQueryResultList
                = Optional.ofNullable(searchResponse.getTopkQueryResultList());
        if (topKQueryResultList.isPresent()) {
            for (io.milvus.client.grpc.TopKQueryResult topKQueryResult : topKQueryResultList.get()) {
                List<SearchResponse.QueryResult> responseQueryResults = new ArrayList<>();
                List<io.milvus.client.grpc.QueryResult> queryResults = topKQueryResult.getQueryResultArraysList();
                for (io.milvus.client.grpc.QueryResult queryResult : queryResults) {
                    SearchResponse.QueryResult responseQueryResult
                            = new SearchResponse.QueryResult(queryResult.getId(), queryResult.getDistance());
                    responseQueryResults.add(responseQueryResult);
                }
                queryResultsList.add(responseQueryResults);
            }
        }
        return queryResultsList;
    }

    /////////////////////Log Functions//////////////////////

    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_BLACK = "\u001B[30m";
    private static final String ANSI_RED = "\u001B[31m";
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_YELLOW = "\u001B[33m";
    private static final String ANSI_BLUE = "\u001B[34m";
    private static final String ANSI_PURPLE = "\u001B[35m";
    private static final String ANSI_CYAN = "\u001B[36m";
    private static final String ANSI_WHITE = "\u001B[37m";

    private static final String ANSI_BRIGHT_BLACK  = "\u001B[90m";
    private static final String ANSI_BRIGHT_RED    = "\u001B[91m";
    private static final String ANSI_BRIGHT_GREEN  = "\u001B[92m";
    private static final String ANSI_BRIGHT_YELLOW = "\u001B[93m";
    private static final String ANSI_BRIGHT_BLUE   = "\u001B[94m";
    private static final String ANSI_BRIGHT_PURPLE = "\u001B[95m";
    private static final String ANSI_BRIGHT_CYAN   = "\u001B[96m";
    private static final String ANSI_BRIGHT_WHITE  = "\u001B[97m";

    private void logInfo(String msg, Object... params) {
        logger.log(Level.INFO, ANSI_YELLOW + msg + ANSI_RESET, params);
    }

    private void logWarning(String msg, Object... params) {
        logger.log(Level.WARNING, ANSI_PURPLE + msg + ANSI_RESET, params);
    }

    private void logSevere(String msg, Object... params) {
        logger.log(Level.SEVERE, ANSI_BRIGHT_PURPLE + msg + ANSI_RESET, params);
    }
}
