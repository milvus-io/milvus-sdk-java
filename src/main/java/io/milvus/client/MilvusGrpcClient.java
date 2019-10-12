/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.milvus.client;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import javax.annotation.Nonnull;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Actual implementation of interface <code>MilvusClient</code> */
public class MilvusGrpcClient implements MilvusClient {

  private static final Logger logger = Logger.getLogger(MilvusGrpcClient.class.getName());
  private static final String ANSI_RESET = "\u001B[0m";
  private static final String ANSI_YELLOW = "\u001B[33m";
  private static final String ANSI_PURPLE = "\u001B[35m";
  private static final String ANSI_BRIGHT_PURPLE = "\u001B[95m";

  private ManagedChannel channel = null;
  private io.milvus.grpc.MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub;

  /////////////////////// Client Calls///////////////////////

  @Override
  public Response connect(ConnectParam connectParam) {
    if (channel != null) {
      logWarning("You have already connected!");
      return new Response(Response.Status.CONNECT_FAILED, "You have already connected!");
    }

    try {
      int port = Integer.parseInt(connectParam.getPort());
      if (port < 0 || port > 0xFFFF) {
        logSevere("Connect failed! Port {0} out of range", connectParam.getPort());
        return new Response(Response.Status.CONNECT_FAILED, "Port " + port + " out of range");
      }

      channel =
          ManagedChannelBuilder.forAddress(connectParam.getHost(), port)
              .usePlaintext()
              .maxInboundMessageSize(Integer.MAX_VALUE)
              .build();

      ConnectivityState connectivityState;
      connectivityState = channel.getState(true);

      logInfo("Waiting to connect...");
      TimeUnit.MILLISECONDS.sleep(500);

      connectivityState = channel.getState(false);
      if (connectivityState != ConnectivityState.READY) {
        logSevere("Connect failed! {0}", connectParam.toString());
        return new Response(
            Response.Status.CONNECT_FAILED, "connectivity state = " + connectivityState);
      }

      blockingStub = io.milvus.grpc.MilvusServiceGrpc.newBlockingStub(channel);

    } catch (Exception e) {
      logSevere("Connect failed! {0}\n{1}", connectParam.toString(), e.toString());
      return new Response(Response.Status.CONNECT_FAILED, e.toString());
    }

    logInfo("Connected successfully!\n{0}", connectParam.toString());
    return new Response(Response.Status.SUCCESS);
  }

  @Override
  public boolean connected() {
    if (channel == null) {
      return false;
    }
    ConnectivityState connectivityState = channel.getState(false);
    return connectivityState == ConnectivityState.READY;
  }

  @Override
  public Response disconnect() throws InterruptedException {
    if (!connected()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    } else {
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
    io.milvus.grpc.TableSchema request =
        io.milvus.grpc.TableSchema.newBuilder()
            .setTableName(tableSchema.getTableName())
            .setDimension(tableSchema.getDimension())
            .setIndexFileSize(tableSchema.getIndexFileSize())
            .setMetricType(tableSchema.getMetricType().getVal())
            .build();

    io.milvus.grpc.Status response;

    try {
      response =
          blockingStub
              .withDeadlineAfter(tableSchemaParam.getTimeout(), TimeUnit.SECONDS)
              .createTable(request);

      if (response.getErrorCode() == io.milvus.grpc.ErrorCode.SUCCESS) {
        logInfo("Created table successfully!\n{0}", tableSchema.toString());
        return new Response(Response.Status.SUCCESS);
      } else if (response.getReason().contentEquals("Table already exists")) {
        logWarning("Table `{0}` already exists", tableSchema.getTableName());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      } else {
        logSevere("Create table failed\n{0}\n{1}", tableSchema.toString(), response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
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
    io.milvus.grpc.TableName request =
        io.milvus.grpc.TableName.newBuilder().setTableName(tableName).build();
    io.milvus.grpc.BoolReply response;

    try {
      response =
          blockingStub
              .withDeadlineAfter(tableParam.getTimeout(), TimeUnit.SECONDS)
              .hasTable(request);

      if (response.getStatus().getErrorCode() == io.milvus.grpc.ErrorCode.SUCCESS) {
        logInfo("hasTable `{0}` = {1}", tableName, response.getBoolReply());
        return new HasTableResponse(new Response(Response.Status.SUCCESS), response.getBoolReply());
      } else {
        logSevere("hasTable `{0}` failed:\n{1}", tableName, response.toString());
        return new HasTableResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
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
    io.milvus.grpc.TableName request =
        io.milvus.grpc.TableName.newBuilder().setTableName(tableName).build();
    io.milvus.grpc.Status response;

    try {
      response =
          blockingStub
              .withDeadlineAfter(tableParam.getTimeout(), TimeUnit.SECONDS)
              .dropTable(request);

      if (response.getErrorCode() == io.milvus.grpc.ErrorCode.SUCCESS) {
        logInfo("Dropped table `{0}` successfully!", tableName);
        return new Response(Response.Status.SUCCESS);
      } else {
        logSevere("Drop table `{0}` failed:\n{1}", tableName, response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logSevere("dropTable RPC failed:\n{0}", e.getStatus().toString());
      return new Response(Response.Status.RPC_ERROR, e.toString());
    }
  }

  @Override
  public Response createIndex(@Nonnull CreateIndexParam createIndexParam) {

    if (!connected()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    io.milvus.grpc.Index index =
        io.milvus.grpc.Index.newBuilder()
            .setIndexType(createIndexParam.getIndex().getIndexType().getVal())
            .setNlist(createIndexParam.getIndex().getNList())
            .build();
    io.milvus.grpc.IndexParam request =
        io.milvus.grpc.IndexParam.newBuilder()
            .setTableName(createIndexParam.getTableName())
            .setIndex(index)
            .build();

    io.milvus.grpc.Status response;

    try {
      response =
          blockingStub
              .withDeadlineAfter(createIndexParam.getTimeout(), TimeUnit.SECONDS)
              .createIndex(request);

      if (response.getErrorCode() == io.milvus.grpc.ErrorCode.SUCCESS) {
        logInfo("Created index successfully!\n{0}", createIndexParam.toString());
        return new Response(Response.Status.SUCCESS);
      } else {
        logSevere(
            "Create index failed\n{0}\n{1}", createIndexParam.toString(), response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
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
      return new InsertResponse(
          new Response(Response.Status.CLIENT_NOT_CONNECTED), new ArrayList<>());
    }

    List<io.milvus.grpc.RowRecord> rowRecordList = new ArrayList<>();
    for (List<Float> vectors : insertParam.getVectors()) {
      io.milvus.grpc.RowRecord rowRecord =
          io.milvus.grpc.RowRecord.newBuilder().addAllVectorData(vectors).build();
      rowRecordList.add(rowRecord);
    }

    io.milvus.grpc.InsertParam request =
        io.milvus.grpc.InsertParam.newBuilder()
            .setTableName(insertParam.getTableName())
            .addAllRowRecordArray(rowRecordList)
            .addAllRowIdArray(insertParam.getVectorIds())
            .build();
    io.milvus.grpc.VectorIds response;

    try {
      response =
          blockingStub
              .withDeadlineAfter(insertParam.getTimeout(), TimeUnit.SECONDS)
              .insert(request);

      if (response.getStatus().getErrorCode() == io.milvus.grpc.ErrorCode.SUCCESS) {
        Optional<List<Long>> resultVectorIds = Optional.ofNullable(response.getVectorIdArrayList());
        logInfo(
            "Inserted {0} vectors to table `{1}` successfully!",
            resultVectorIds.map(List::size).orElse(0), insertParam.getTableName());
        return new InsertResponse(
            new Response(Response.Status.SUCCESS), resultVectorIds.orElse(new ArrayList<>()));
      } else {
        logSevere("Insert vectors failed:\n{0}", response.toString());
        return new InsertResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()),
            new ArrayList<>());
      }
    } catch (StatusRuntimeException e) {
      logSevere("insert RPC failed:\n{0}", e.getStatus().toString());
      return new InsertResponse(
          new Response(Response.Status.RPC_ERROR, e.toString()), new ArrayList<>());
    }
  }

  @Override
  public SearchResponse search(@Nonnull SearchParam searchParam) {

    if (!connected()) {
      logWarning("You are not connected to Milvus server");
      return new SearchResponse(
          new Response(Response.Status.CLIENT_NOT_CONNECTED), new ArrayList<>());
    }

    List<io.milvus.grpc.RowRecord> queryRowRecordList = getQueryRowRecordList(searchParam);

    List<io.milvus.grpc.Range> queryRangeList = getQueryRangeList(searchParam);

    io.milvus.grpc.SearchParam request =
        io.milvus.grpc.SearchParam.newBuilder()
            .setTableName(searchParam.getTableName())
            .addAllQueryRecordArray(queryRowRecordList)
            .addAllQueryRangeArray(queryRangeList)
            .setTopk(searchParam.getTopK())
            .setNprobe(searchParam.getNProbe())
            .build();

    io.milvus.grpc.TopKQueryResultList response;

    try {
      response =
          blockingStub
              .withDeadlineAfter(searchParam.getTimeout(), TimeUnit.SECONDS)
              .search(request);

      if (response.getStatus().getErrorCode() == io.milvus.grpc.ErrorCode.SUCCESS) {
        List<List<SearchResponse.QueryResult>> queryResultsList = getQueryResultsList(response);
        logInfo(
            "Search completed successfully! Returned results for {0} queries",
            queryResultsList.size());
        return new SearchResponse(new Response(Response.Status.SUCCESS), queryResultsList);
      } else {
        logSevere("Search failed:\n{0}", response.toString());
        return new SearchResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()),
            new ArrayList<>());
      }
    } catch (StatusRuntimeException e) {
      logSevere("search RPC failed:\n{0}", e.getStatus().toString());
      return new SearchResponse(
          new Response(Response.Status.RPC_ERROR, e.toString()), new ArrayList<>());
    }
  }

  @Override
  public SearchResponse searchInFiles(@Nonnull SearchInFilesParam searchInFilesParam) {

    if (!connected()) {
      logWarning("You are not connected to Milvus server");
      return new SearchResponse(
          new Response(Response.Status.CLIENT_NOT_CONNECTED), new ArrayList<>());
    }

    SearchParam searchParam = searchInFilesParam.getSearchParam();

    List<io.milvus.grpc.RowRecord> queryRowRecordList = getQueryRowRecordList(searchParam);

    List<io.milvus.grpc.Range> queryRangeList = getQueryRangeList(searchParam);

    io.milvus.grpc.SearchParam searchParamToSet =
        io.milvus.grpc.SearchParam.newBuilder()
            .setTableName(searchParam.getTableName())
            .addAllQueryRecordArray(queryRowRecordList)
            .addAllQueryRangeArray(queryRangeList)
            .setTopk(searchParam.getTopK())
            .setNprobe(searchParam.getNProbe())
            .build();

    io.milvus.grpc.SearchInFilesParam request =
        io.milvus.grpc.SearchInFilesParam.newBuilder()
            .addAllFileIdArray(searchInFilesParam.getFileIds())
            .setSearchParam(searchParamToSet)
            .build();

    io.milvus.grpc.TopKQueryResultList response;

    try {
      response =
          blockingStub
              .withDeadlineAfter(searchInFilesParam.getTimeout(), TimeUnit.SECONDS)
              .searchInFiles(request);

      if (response.getStatus().getErrorCode() == io.milvus.grpc.ErrorCode.SUCCESS) {
        logInfo("Search in files {0} completed successfully!", searchInFilesParam.getFileIds());

        List<List<SearchResponse.QueryResult>> queryResultsList = getQueryResultsList(response);
        return new SearchResponse(new Response(Response.Status.SUCCESS), queryResultsList);
      } else {
        logSevere(
            "Search in files {0} failed:\n{1}",
            searchInFilesParam.getFileIds(), response.toString());
        return new SearchResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()),
            new ArrayList<>());
      }
    } catch (StatusRuntimeException e) {
      logSevere("searchInFiles RPC failed:\n{0}", e.getStatus().toString());
      return new SearchResponse(
          new Response(Response.Status.RPC_ERROR, e.toString()), new ArrayList<>());
    }
  }

  @Override
  public DescribeTableResponse describeTable(@Nonnull TableParam tableParam) {

    if (!connected()) {
      logWarning("You are not connected to Milvus server");
      return new DescribeTableResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED), null);
    }

    String tableName = tableParam.getTableName();
    io.milvus.grpc.TableName request =
        io.milvus.grpc.TableName.newBuilder().setTableName(tableName).build();
    io.milvus.grpc.TableSchema response;

    try {
      response =
          blockingStub
              .withDeadlineAfter(tableParam.getTimeout(), TimeUnit.SECONDS)
              .describeTable(request);

      if (response.getStatus().getErrorCode() == io.milvus.grpc.ErrorCode.SUCCESS) {
        TableSchema tableSchema =
            new TableSchema.Builder(response.getTableName(), response.getDimension())
                .withIndexFileSize(response.getIndexFileSize())
                .withMetricType(MetricType.valueOf(response.getMetricType()))
                .build();
        logInfo("Describe Table `{0}` returned:\n{1}", tableName, tableSchema);
        return new DescribeTableResponse(new Response(Response.Status.SUCCESS), tableSchema);
      } else {
        logSevere("Describe Table `{0}` failed:\n{1}", tableName, response.toString());
        return new DescribeTableResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
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
      return new ShowTablesResponse(
          new Response(Response.Status.CLIENT_NOT_CONNECTED), new ArrayList<>());
    }

    io.milvus.grpc.Command request = io.milvus.grpc.Command.newBuilder().setCmd("").build();
    io.milvus.grpc.TableNameList response;

    try {
      response = blockingStub.showTables(request);

      if (response.getStatus().getErrorCode() == io.milvus.grpc.ErrorCode.SUCCESS) {
        List<String> tableNames = response.getTableNamesList();
        logInfo("Current tables: {0}", tableNames.toString());
        return new ShowTablesResponse(new Response(Response.Status.SUCCESS), tableNames);
      } else {
        logSevere("Show tables failed:\n{0}", response.toString());
        return new ShowTablesResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()),
            new ArrayList<>());
      }
    } catch (StatusRuntimeException e) {
      logSevere("showTables RPC failed:\n{0}", e.getStatus().toString());
      return new ShowTablesResponse(
          new Response(Response.Status.RPC_ERROR, e.toString()), new ArrayList<>());
    }
  }

  @Override
  public GetTableRowCountResponse getTableRowCount(@Nonnull TableParam tableParam) {

    if (!connected()) {
      logWarning("You are not connected to Milvus server");
      return new GetTableRowCountResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED), 0);
    }

    String tableName = tableParam.getTableName();
    io.milvus.grpc.TableName request =
        io.milvus.grpc.TableName.newBuilder().setTableName(tableName).build();
    io.milvus.grpc.TableRowCount response;

    try {
      response =
          blockingStub
              .withDeadlineAfter(tableParam.getTimeout(), TimeUnit.SECONDS)
              .countTable(request);

      if (response.getStatus().getErrorCode() == io.milvus.grpc.ErrorCode.SUCCESS) {
        long tableRowCount = response.getTableRowCount();
        logInfo("Table `{0}` has {1} rows", tableName, tableRowCount);
        return new GetTableRowCountResponse(new Response(Response.Status.SUCCESS), tableRowCount);
      } else {
        logSevere("Get table `{0}` row count failed:\n{1}", tableName, response.toString());
        return new GetTableRowCountResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()),
            0);
      }
    } catch (StatusRuntimeException e) {
      logSevere("countTable RPC failed:\n{0}", e.getStatus().toString());
      return new GetTableRowCountResponse(new Response(Response.Status.RPC_ERROR, e.toString()), 0);
    }
  }

  @Override
  public Response serverStatus() {
    CommandParam commandParam = new CommandParam.Builder("OK").build();
    return command(commandParam);
  }

  @Override
  public Response serverVersion() {
    CommandParam commandParam = new CommandParam.Builder("version").build();
    return command(commandParam);
  }

  private Response command(@Nonnull CommandParam commandParam) {

    if (!connected()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    String command = commandParam.getCommand();
    io.milvus.grpc.Command request = io.milvus.grpc.Command.newBuilder().setCmd(command).build();
    io.milvus.grpc.StringReply response;

    try {
      response =
          blockingStub.withDeadlineAfter(commandParam.getTimeout(), TimeUnit.SECONDS).cmd(request);

      if (response.getStatus().getErrorCode() == io.milvus.grpc.ErrorCode.SUCCESS) {
        logInfo("Command `{0}`: {1}", command, response.getStringReply());
        return new Response(Response.Status.SUCCESS, response.getStringReply());
      } else {
        logSevere("Command `{0}` failed:\n{1}", command, response.toString());
        return new Response(
            Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
            response.getStatus().getReason());
      }
    } catch (StatusRuntimeException e) {
      logSevere("Command RPC failed:\n{0}", e.getStatus().toString());
      return new Response(Response.Status.RPC_ERROR, e.toString());
    }
  }

  public Response deleteByRange(@Nonnull DeleteByRangeParam deleteByRangeParam) {

    if (!connected()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    io.milvus.grpc.DeleteByRangeParam request =
        io.milvus.grpc.DeleteByRangeParam.newBuilder()
            .setRange(getRange(deleteByRangeParam.getDateRange()))
            .setTableName(deleteByRangeParam.getTableName())
            .build();
    io.milvus.grpc.Status response;

    try {
      response =
          blockingStub
              .withDeadlineAfter(deleteByRangeParam.getTimeout(), TimeUnit.SECONDS)
              .deleteByRange(request);

      if (response.getErrorCode() == io.milvus.grpc.ErrorCode.SUCCESS) {
        logInfo(
            "Deleted vectors from table `{0}` in range {1} successfully!",
            deleteByRangeParam.getTableName(), deleteByRangeParam.getDateRange().toString());
        return new Response(Response.Status.SUCCESS);
      } else {
        logSevere(
            "Deleted vectors from table `{0}` in range {1} failed:\n{2}",
            deleteByRangeParam.getTableName(),
            deleteByRangeParam.getDateRange().toString(),
            response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
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
    io.milvus.grpc.TableName request =
        io.milvus.grpc.TableName.newBuilder().setTableName(tableName).build();
    io.milvus.grpc.Status response;

    try {
      response =
          blockingStub
              .withDeadlineAfter(tableParam.getTimeout(), TimeUnit.SECONDS)
              .preloadTable(request);

      if (response.getErrorCode() == io.milvus.grpc.ErrorCode.SUCCESS) {
        logInfo("Preloaded table `{0}` successfully!", tableName);
        return new Response(Response.Status.SUCCESS);
      } else {
        logSevere("Preload table `{0}` failed:\n{1}", tableName, response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
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
    io.milvus.grpc.TableName request =
        io.milvus.grpc.TableName.newBuilder().setTableName(tableName).build();
    io.milvus.grpc.IndexParam response;

    try {
      response =
          blockingStub
              .withDeadlineAfter(tableParam.getTimeout(), TimeUnit.SECONDS)
              .describeIndex(request);

      if (response.getStatus().getErrorCode() == io.milvus.grpc.ErrorCode.SUCCESS) {
        Index index =
            new Index.Builder()
                .withIndexType(IndexType.valueOf(response.getIndex().getIndexType()))
                .withNList(response.getIndex().getNlist())
                .build();
        logInfo("Describe index for table `{0}` returned:\n{1}", tableName, index.toString());
        return new DescribeIndexResponse(new Response(Response.Status.SUCCESS), index);
      } else {
        logSevere("Describe index for table `{0}` failed:\n{1}", tableName, response.toString());
        return new DescribeIndexResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
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
    io.milvus.grpc.TableName request =
        io.milvus.grpc.TableName.newBuilder().setTableName(tableName).build();
    io.milvus.grpc.Status response;

    try {
      response =
          blockingStub
              .withDeadlineAfter(tableParam.getTimeout(), TimeUnit.SECONDS)
              .dropIndex(request);

      if (response.getErrorCode() == io.milvus.grpc.ErrorCode.SUCCESS) {
        logInfo("Dropped index for table `{0}` successfully!", tableName);
        return new Response(Response.Status.SUCCESS);
      } else {
        logSevere("Drop index for table `{0}` failed:\n{1}", tableName, response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logSevere("dropIndex RPC failed:\n{0}", e.getStatus().toString());
      return new Response(Response.Status.RPC_ERROR, e.toString());
    }
  }

  ///////////////////// Util Functions/////////////////////
  private List<io.milvus.grpc.RowRecord> getQueryRowRecordList(@Nonnull SearchParam searchParam) {
    List<io.milvus.grpc.RowRecord> queryRowRecordList = new ArrayList<>();
    for (List<Float> vectors : searchParam.getQueryVectors()) {
      io.milvus.grpc.RowRecord rowRecord =
          io.milvus.grpc.RowRecord.newBuilder().addAllVectorData(vectors).build();
      queryRowRecordList.add(rowRecord);
    }
    return queryRowRecordList;
  }

  private List<io.milvus.grpc.Range> getQueryRangeList(@Nonnull SearchParam searchParam) {
    List<io.milvus.grpc.Range> queryRangeList = new ArrayList<>();
    String datePattern = "yyyy-MM-dd";
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(datePattern);
    for (DateRange queryRange : searchParam.getdateRanges()) {
      io.milvus.grpc.Range dateRange =
          io.milvus.grpc.Range.newBuilder()
              .setStartValue(simpleDateFormat.format(queryRange.getStartDate()))
              .setEndValue(simpleDateFormat.format(queryRange.getEndDate()))
              .build();
      queryRangeList.add(dateRange);
    }
    return queryRangeList;
  }

  private io.milvus.grpc.Range getRange(@Nonnull DateRange dateRange) {
    String datePattern = "yyyy-MM-dd";
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(datePattern);
    return io.milvus.grpc.Range.newBuilder()
        .setStartValue(simpleDateFormat.format(dateRange.getStartDate()))
        .setEndValue(simpleDateFormat.format(dateRange.getEndDate()))
        .build();
  }

  private List<List<SearchResponse.QueryResult>> getQueryResultsList(
      io.milvus.grpc.TopKQueryResultList searchResponse) {
    // TODO: refactor
    List<List<SearchResponse.QueryResult>> queryResultsList = new ArrayList<>();
    Optional<List<io.milvus.grpc.TopKQueryResult>> topKQueryResultList =
        Optional.ofNullable(searchResponse.getTopkQueryResultList());
    if (topKQueryResultList.isPresent()) {
      for (io.milvus.grpc.TopKQueryResult topKQueryResult : topKQueryResultList.get()) {
        List<SearchResponse.QueryResult> responseQueryResults = new ArrayList<>();
        List<io.milvus.grpc.QueryResult> queryResults = topKQueryResult.getQueryResultArraysList();
        for (io.milvus.grpc.QueryResult queryResult : queryResults) {
          SearchResponse.QueryResult responseQueryResult =
              new SearchResponse.QueryResult(queryResult.getId(), queryResult.getDistance());
          responseQueryResults.add(responseQueryResult);
        }
        queryResultsList.add(responseQueryResults);
      }
    }
    return queryResultsList;
  }

  ///////////////////// Log Functions//////////////////////

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
