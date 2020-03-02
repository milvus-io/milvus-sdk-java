/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.milvus.client;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.milvus.grpc.PartitionParam;
import org.apache.commons.collections4.ListUtils;

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
  private io.milvus.grpc.MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub = null;

  //Constructor
  public MilvusGrpcClient() {
    logger.setLevel(Level.ALL);
  }

  public MilvusGrpcClient(Level logLevel) {
    logger.setLevel(logLevel);
  }

  /////////////////////// Client Calls///////////////////////

  @Override
  public Response connect(ConnectParam connectParam) throws ConnectFailedException {
    if (channel != null && !(channel.isShutdown() || channel.isTerminated())) {
      logWarning("Channel is not shutdown or terminated");
      throw new ConnectFailedException("Channel is not shutdown or terminated");
    }

    try {

      channel =
          ManagedChannelBuilder.forAddress(
                  connectParam.getHost(), Integer.parseInt(connectParam.getPort()))
              .usePlaintext()
              .maxInboundMessageSize(Integer.MAX_VALUE)
              .keepAliveTime(
                  connectParam.getKeepAliveTime(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS)
              .keepAliveTimeout(
                  connectParam.getKeepAliveTimeout(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS)
              .keepAliveWithoutCalls(connectParam.isKeepAliveWithoutCalls())
              .idleTimeout(connectParam.getIdleTimeout(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS)
              .build();

      channel.getState(true);

      long timeout = connectParam.getConnectTimeout(TimeUnit.MILLISECONDS);
      logInfo("Trying to connect...Timeout in {0} ms", timeout);

      final long checkFrequency = 100; // ms
      while (channel.getState(false) != ConnectivityState.READY) {
        if (timeout <= 0) {
          logSevere("Connect timeout! {0}", connectParam.toString());
          throw new ConnectFailedException("Connect timeout");
        }
        TimeUnit.MILLISECONDS.sleep(checkFrequency);
        timeout -= checkFrequency;
      }

      blockingStub = io.milvus.grpc.MilvusServiceGrpc.newBlockingStub(channel);

    } catch (Exception e) {
      if (!(e instanceof ConnectFailedException)) {
        logSevere("Connect failed! {0}\n{1}", connectParam.toString(), e.toString());
      }
      throw new ConnectFailedException("Exception occurred: " + e.toString());
    }

    logInfo(
        "Connection established successfully to host={0}, port={1}",
        connectParam.getHost(), connectParam.getPort());
    return new Response(Response.Status.SUCCESS);
  }

  @Override
  public boolean isConnected() {
    if (channel == null) {
      return false;
    }
    ConnectivityState connectivityState = channel.getState(false);
    return connectivityState == ConnectivityState.READY;
  }

  @Override
  public Response disconnect() throws InterruptedException {
    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    } else {
      try {
        if (channel.shutdown().awaitTermination(60, TimeUnit.SECONDS)) {
          logInfo("Channel terminated");
        } else {
          logSevere("Encountered error when terminating channel");
          return new Response(Response.Status.RPC_ERROR);
        }
      } catch (InterruptedException e) {
        logSevere("Exception thrown when terminating channel: {0}", e.toString());
        throw e;
      }
    }
    return new Response(Response.Status.SUCCESS);
  }

  @Override
  public Response createTable(@Nonnull TableSchema tableSchema) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    io.milvus.grpc.TableSchema request =
        io.milvus.grpc.TableSchema.newBuilder()
            .setTableName(tableSchema.getTableName())
            .setDimension(tableSchema.getDimension())
            .setIndexFileSize(tableSchema.getIndexFileSize())
            .setMetricType(tableSchema.getMetricType().getVal())
            .build();

    io.milvus.grpc.Status response;

    try {
      response = blockingStub.createTable(request);

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
  public HasTableResponse hasTable(@Nonnull String tableName) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new HasTableResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED), false);
    }

    io.milvus.grpc.TableName request =
        io.milvus.grpc.TableName.newBuilder().setTableName(tableName).build();
    io.milvus.grpc.BoolReply response;

    try {
      response = blockingStub.hasTable(request);

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
  public Response dropTable(@Nonnull String tableName) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    io.milvus.grpc.TableName request =
        io.milvus.grpc.TableName.newBuilder().setTableName(tableName).build();
    io.milvus.grpc.Status response;

    try {
      response = blockingStub.dropTable(request);

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

    if (!channelIsReadyOrIdle()) {
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
      response = blockingStub.createIndex(request);

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
  public Response createPartition(Partition partition) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    io.milvus.grpc.PartitionParam request =
        io.milvus.grpc.PartitionParam.newBuilder()
            .setTableName(partition.getTableName())
            .setPartitionName(partition.getPartitionName())
            .setTag(partition.getTag())
            .build();

    io.milvus.grpc.Status response;

    try {
      response = blockingStub.createPartition(request);

      if (response.getErrorCode() == io.milvus.grpc.ErrorCode.SUCCESS) {
        logInfo("Created partition successfully!\n{0}", partition.toString());
        return new Response(Response.Status.SUCCESS);
      } else {
        logSevere("Create partition failed\n{0}\n{1}", partition.toString(), response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logSevere("createPartition RPC failed:\n{0}", e.getStatus().toString());
      return new Response(Response.Status.RPC_ERROR, e.toString());
    }
  }

  @Override
  public ShowPartitionsResponse showPartitions(String tableName) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new ShowPartitionsResponse(
          new Response(Response.Status.CLIENT_NOT_CONNECTED), new ArrayList<>());
    }

    io.milvus.grpc.TableName request =
        io.milvus.grpc.TableName.newBuilder().setTableName(tableName).build();
    io.milvus.grpc.PartitionList response;

    try {
      response = blockingStub.showPartitions(request);

      if (response.getStatus().getErrorCode() == io.milvus.grpc.ErrorCode.SUCCESS) {
        List<PartitionParam> partitionList = response.getPartitionArrayList();
        List<Partition> partitions = new ArrayList<>();
        for (PartitionParam partitionParam : partitionList) {
          partitions.add(
              new Partition.Builder(
                      partitionParam.getTableName(),
                      partitionParam.getPartitionName(),
                      partitionParam.getTag())
                  .build());
        }
        logInfo("Current partitions of table {0}: {1}", tableName, partitions.toString());
        return new ShowPartitionsResponse(new Response(Response.Status.SUCCESS), partitions);
      } else {
        logSevere("Show partitions failed:\n{0}", response.toString());
        return new ShowPartitionsResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()),
            new ArrayList<>());
      }
    } catch (StatusRuntimeException e) {
      logSevere("showPartitions RPC failed:\n{0}", e.getStatus().toString());
      return new ShowPartitionsResponse(
          new Response(Response.Status.RPC_ERROR, e.toString()), new ArrayList<>());
    }
  }

  @Override
  public Response dropPartition(String partitionName) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    io.milvus.grpc.PartitionParam request =
        io.milvus.grpc.PartitionParam.newBuilder().setPartitionName(partitionName).build();
    io.milvus.grpc.Status response;

    try {
      response = blockingStub.dropPartition(request);

      if (response.getErrorCode() == io.milvus.grpc.ErrorCode.SUCCESS) {
        logInfo("Dropped partition `{0}` successfully!", partitionName);
        return new Response(Response.Status.SUCCESS);
      } else {
        logSevere("Drop partition `{0}` failed:\n{1}", partitionName, response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logSevere("dropPartition RPC failed:\n{0}", e.getStatus().toString());
      return new Response(Response.Status.RPC_ERROR, e.toString());
    }
  }

  @Override
  public Response dropPartition(String tableName, String tag) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    io.milvus.grpc.PartitionParam request =
        io.milvus.grpc.PartitionParam.newBuilder().setTableName(tableName).setTag(tag).build();
    io.milvus.grpc.Status response;

    try {
      response = blockingStub.dropPartition(request);

      if (response.getErrorCode() == io.milvus.grpc.ErrorCode.SUCCESS) {
        logInfo("Dropped partition of table `{0}` and tag `{1}` successfully!", tableName, tag);
        return new Response(Response.Status.SUCCESS);
      } else {
        logSevere(
            "Drop partition of table `{0}` and tag `{1}` failed:\n{1}",
            tableName, tag, response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logSevere("dropPartition RPC failed:\n{0}", e.getStatus().toString());
      return new Response(Response.Status.RPC_ERROR, e.toString());
    }
  }

  @Override
  public InsertResponse insert(@Nonnull InsertParam insertParam) {

    if (!channelIsReadyOrIdle()) {
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
            .setPartitionTag(insertParam.getPartitionTag())
            .build();
    io.milvus.grpc.VectorIds response;

    try {
      response = blockingStub.insert(request);

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

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      SearchResponse searchResponse = new SearchResponse();
      searchResponse.setResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED));
      return searchResponse;
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
            .addAllPartitionTagArray(searchParam.getPartitionTags())
            .build();

    io.milvus.grpc.TopKQueryResult response;

    try {
      response = blockingStub.search(request);

      if (response.getStatus().getErrorCode() == io.milvus.grpc.ErrorCode.SUCCESS) {
        SearchResponse searchResponse = buildSearchResponse(response);
        searchResponse.setResponse(new Response(Response.Status.SUCCESS));
        logInfo(
            "Search completed successfully! Returned results for {0} queries",
            searchResponse.getNumQueries());
        return searchResponse;
      } else {
        logSevere("Search failed:\n{0}", response.toString());
        SearchResponse searchResponse = new SearchResponse();
        searchResponse.setResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()));
        return searchResponse;
      }
    } catch (StatusRuntimeException e) {
      logSevere("search RPC failed:\n{0}", e.getStatus().toString());
      SearchResponse searchResponse = new SearchResponse();
      searchResponse.setResponse(new Response(Response.Status.RPC_ERROR, e.toString()));
      return searchResponse;
    }
  }

  @Override
  public SearchResponse searchInFiles(@Nonnull SearchInFilesParam searchInFilesParam) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      SearchResponse searchResponse = new SearchResponse();
      searchResponse.setResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED));
      return searchResponse;
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
            .addAllPartitionTagArray(searchParam.getPartitionTags())
            .build();

    io.milvus.grpc.SearchInFilesParam request =
        io.milvus.grpc.SearchInFilesParam.newBuilder()
            .addAllFileIdArray(searchInFilesParam.getFileIds())
            .setSearchParam(searchParamToSet)
            .build();

    io.milvus.grpc.TopKQueryResult response;

    try {
      response = blockingStub.searchInFiles(request);

      if (response.getStatus().getErrorCode() == io.milvus.grpc.ErrorCode.SUCCESS) {
        SearchResponse searchResponse = buildSearchResponse(response);
        searchResponse.setResponse(new Response(Response.Status.SUCCESS));
        logInfo("Search in files {0} completed successfully!", searchInFilesParam.getFileIds());
        return searchResponse;
      } else {
        logSevere(
            "Search in files {0} failed:\n{1}",
            searchInFilesParam.getFileIds(), response.toString());

        SearchResponse searchResponse = new SearchResponse();
        searchResponse.setResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()));
        return searchResponse;
      }
    } catch (StatusRuntimeException e) {
      logSevere("searchInFiles RPC failed:\n{0}", e.getStatus().toString());
      SearchResponse searchResponse = new SearchResponse();
      searchResponse.setResponse(new Response(Response.Status.RPC_ERROR, e.toString()));
      return searchResponse;
    }
  }

  @Override
  public DescribeTableResponse describeTable(@Nonnull String tableName) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new DescribeTableResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED), null);
    }

    io.milvus.grpc.TableName request =
        io.milvus.grpc.TableName.newBuilder().setTableName(tableName).build();
    io.milvus.grpc.TableSchema response;

    try {
      response = blockingStub.describeTable(request);

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

    if (!channelIsReadyOrIdle()) {
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
  public GetTableRowCountResponse getTableRowCount(@Nonnull String tableName) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new GetTableRowCountResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED), 0);
    }

    io.milvus.grpc.TableName request =
        io.milvus.grpc.TableName.newBuilder().setTableName(tableName).build();
    io.milvus.grpc.TableRowCount response;

    try {
      response = blockingStub.countTable(request);

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
  public Response getServerStatus() {
    return command("OK");
  }

  @Override
  public Response getServerVersion() {
    return command("version");
  }

  private Response command(@Nonnull String command) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    io.milvus.grpc.Command request = io.milvus.grpc.Command.newBuilder().setCmd(command).build();
    io.milvus.grpc.StringReply response;

    try {
      response = blockingStub.cmd(request);

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

  // TODO: make deleteByRange private for now
  private Response deleteByRange(@Nonnull String tableName, @Nonnull DateRange dateRange) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    io.milvus.grpc.DeleteByRangeParam request =
        io.milvus.grpc.DeleteByRangeParam.newBuilder()
            .setRange(getRange(dateRange))
            .setTableName(tableName)
            .build();
    io.milvus.grpc.Status response;

    try {
      response = blockingStub.deleteByRange(request);

      if (response.getErrorCode() == io.milvus.grpc.ErrorCode.SUCCESS) {
        logInfo(
            "Deleted vectors from table `{0}` in range {1} successfully!", tableName, dateRange);
        return new Response(Response.Status.SUCCESS);
      } else {
        logSevere(
            "Deleted vectors from table `{0}` in range {1} failed:\n{2}",
            tableName, dateRange, response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logSevere("deleteByRange RPC failed:\n{0}", e.getStatus().toString());
      return new Response(Response.Status.RPC_ERROR, e.toString());
    }
  }

  @Override
  public Response preloadTable(@Nonnull String tableName) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    io.milvus.grpc.TableName request =
        io.milvus.grpc.TableName.newBuilder().setTableName(tableName).build();
    io.milvus.grpc.Status response;

    try {
      response = blockingStub.preloadTable(request);

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
  public DescribeIndexResponse describeIndex(@Nonnull String tableName) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new DescribeIndexResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED), null);
    }

    io.milvus.grpc.TableName request =
        io.milvus.grpc.TableName.newBuilder().setTableName(tableName).build();
    io.milvus.grpc.IndexParam response;

    try {
      response = blockingStub.describeIndex(request);

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
  public Response dropIndex(@Nonnull String tableName) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    io.milvus.grpc.TableName request =
        io.milvus.grpc.TableName.newBuilder().setTableName(tableName).build();
    io.milvus.grpc.Status response;

    try {
      response = blockingStub.dropIndex(request);

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
    for (DateRange queryRange : searchParam.getDateRanges()) {
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

  private SearchResponse buildSearchResponse(io.milvus.grpc.TopKQueryResult topKQueryResult) {

    final int numQueries = (int) topKQueryResult.getRowNum();
    final int topK =
        numQueries == 0
            ? 0
            : topKQueryResult.getIdsCount()
                / numQueries; // Guaranteed to be divisible from server side

    List<List<Long>> resultIdsList = new ArrayList<>();
    List<List<Float>> resultDistancesList = new ArrayList<>();
    if (topK > 0) {
      resultIdsList = ListUtils.partition(topKQueryResult.getIdsList(), topK);
      resultDistancesList = ListUtils.partition(topKQueryResult.getDistancesList(), topK);
    }

    SearchResponse searchResponse = new SearchResponse();
    searchResponse.setNumQueries(numQueries);
    searchResponse.setTopK(topK);
    searchResponse.setResultIdsList(resultIdsList);
    searchResponse.setResultDistancesList(resultDistancesList);

    return searchResponse;
  }

  private boolean channelIsReadyOrIdle() {
    if (channel == null) {
      return false;
    }
    ConnectivityState connectivityState = channel.getState(false);
    return connectivityState == ConnectivityState.READY
        || connectivityState
            == ConnectivityState.IDLE; // Since a new RPC would take the channel out of idle mode
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
