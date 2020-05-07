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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.milvus.grpc.*;
import org.apache.commons.collections4.ListUtils;

import javax.annotation.Nonnull;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Actual implementation of interface <code>MilvusClient</code> */
public class MilvusGrpcClient implements MilvusClient {

  private static final Logger logger = Logger.getLogger(MilvusGrpcClient.class.getName());
  private static final String ANSI_RESET = "\u001B[0m";
  private static final String ANSI_YELLOW = "\u001B[33m";
  private static final String ANSI_PURPLE = "\u001B[35m";
  private static final String ANSI_BRIGHT_PURPLE = "\u001B[95m";
  private final String extraParamKey = "params";
  private ManagedChannel channel = null;
  private MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub = null;
  private MilvusServiceGrpc.MilvusServiceFutureStub futureStub = null;

  ////////////////////// Constructor //////////////////////
  public MilvusGrpcClient() {
    logger.setLevel(Level.ALL);
  }

  /**
   * @param logLevel we currently have three levels of logs: <code>INFO</code>, <code>WARNING</code>
   *     and <code>SEVERE</code>. You can also specify to be <code>Level.All</code> or <code>
   *     Level.OFF</code>
   * @see Level
   */
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
          ManagedChannelBuilder.forAddress(connectParam.getHost(), connectParam.getPort())
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
          logSevere("Connect timeout!");
          throw new ConnectFailedException("Connect timeout");
        }
        TimeUnit.MILLISECONDS.sleep(checkFrequency);
        timeout -= checkFrequency;
      }

      blockingStub = MilvusServiceGrpc.newBlockingStub(channel);
      futureStub = MilvusServiceGrpc.newFutureStub(channel);

    } catch (Exception e) {
      if (!(e instanceof ConnectFailedException)) {
        logSevere("Connect failed! {0}", e.toString());
      }
      throw new ConnectFailedException("Exception occurred: " + e.toString());
    }

    logInfo(
        "Connection established successfully to host={0}, port={1}",
        connectParam.getHost(), String.valueOf(connectParam.getPort()));
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
  public Response createCollection(@Nonnull CollectionMapping collectionMapping) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    CollectionSchema request =
        CollectionSchema.newBuilder()
            .setCollectionName(collectionMapping.getCollectionName())
            .setDimension(collectionMapping.getDimension())
            .setIndexFileSize(collectionMapping.getIndexFileSize())
            .setMetricType(collectionMapping.getMetricType().getVal())
            .build();

    Status response;

    try {
      response = blockingStub.createCollection(request);

      if (response.getErrorCode() == ErrorCode.SUCCESS) {
        logInfo("Created collection successfully!\n{0}", collectionMapping.toString());
        return new Response(Response.Status.SUCCESS);
      } else if (response.getReason().contentEquals("Collection already exists")) {
        logWarning("Collection `{0}` already exists", collectionMapping.getCollectionName());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      } else {
        logSevere(
            "Create collection failed\n{0}\n{1}",
            collectionMapping.toString(), response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logSevere("createCollection RPC failed:\n{0}", e.getStatus().toString());
      return new Response(Response.Status.RPC_ERROR, e.toString());
    }
  }

  @Override
  public HasCollectionResponse hasCollection(@Nonnull String collectionName) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new HasCollectionResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED), false);
    }

    CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();
    BoolReply response;

    try {
      response = blockingStub.hasCollection(request);

      if (response.getStatus().getErrorCode() == ErrorCode.SUCCESS) {
        logInfo("hasCollection `{0}` = {1}", collectionName, response.getBoolReply());
        return new HasCollectionResponse(
            new Response(Response.Status.SUCCESS), response.getBoolReply());
      } else {
        logSevere("hasCollection `{0}` failed:\n{1}", collectionName, response.toString());
        return new HasCollectionResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()),
            false);
      }
    } catch (StatusRuntimeException e) {
      logSevere("hasCollection RPC failed:\n{0}", e.getStatus().toString());
      return new HasCollectionResponse(
          new Response(Response.Status.RPC_ERROR, e.toString()), false);
    }
  }

  @Override
  public Response dropCollection(@Nonnull String collectionName) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();
    Status response;

    try {
      response = blockingStub.dropCollection(request);

      if (response.getErrorCode() == ErrorCode.SUCCESS) {
        logInfo("Dropped collection `{0}` successfully!", collectionName);
        return new Response(Response.Status.SUCCESS);
      } else {
        logSevere("Drop collection `{0}` failed:\n{1}", collectionName, response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logSevere("dropCollection RPC failed:\n{0}", e.getStatus().toString());
      return new Response(Response.Status.RPC_ERROR, e.toString());
    }
  }

  @Override
  public Response createIndex(@Nonnull Index index) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    KeyValuePair extraParam =
        KeyValuePair.newBuilder().setKey(extraParamKey).setValue(index.getParamsInJson()).build();
    IndexParam request =
        IndexParam.newBuilder()
            .setCollectionName(index.getCollectionName())
            .setIndexType(index.getIndexType().getVal())
            .addExtraParams(extraParam)
            .build();

    Status response;

    try {
      response = blockingStub.createIndex(request);

      if (response.getErrorCode() == ErrorCode.SUCCESS) {
        logInfo("Created index successfully!\n{0}", index.toString());
        return new Response(Response.Status.SUCCESS);
      } else {
        logSevere("Create index failed:\n{0}\n{1}", index.toString(), response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logSevere("createIndex RPC failed:\n{0}", e.getStatus().toString());
      return new Response(Response.Status.RPC_ERROR, e.toString());
    }
  }

  @Override
  public ListenableFuture<Response> createIndexAsync(@Nonnull Index index) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return Futures.immediateFuture(new Response(Response.Status.CLIENT_NOT_CONNECTED));
    }

    KeyValuePair extraParam =
        KeyValuePair.newBuilder().setKey(extraParamKey).setValue(index.getParamsInJson()).build();
    IndexParam request =
        IndexParam.newBuilder()
            .setCollectionName(index.getCollectionName())
            .setIndexType(index.getIndexType().getVal())
            .addExtraParams(extraParam)
            .build();

    ListenableFuture<Status> response;

    response = futureStub.createIndex(request);

    Futures.addCallback(
        response,
        new FutureCallback<Status>() {
          @Override
          public void onSuccess(Status result) {
            if (result.getErrorCode() == ErrorCode.SUCCESS) {
              logInfo("Created index successfully!\n{0}", index.toString());
            } else {
              logSevere("CreateIndexAsync failed:\n{0}\n{1}", index.toString(), result.toString());
            }
          }

          @Override
          public void onFailure(Throwable t) {
            logSevere("CreateIndexAsync failed:\n{0}", t.getMessage());
          }
        },
        MoreExecutors.directExecutor());

    return Futures.transform(
        response, transformStatusToResponseFunc::apply, MoreExecutors.directExecutor());
  }

  @Override
  public Response createPartition(String collectionName, String tag) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    PartitionParam request =
        PartitionParam.newBuilder().setCollectionName(collectionName).setTag(tag).build();

    Status response;

    try {
      response = blockingStub.createPartition(request);

      if (response.getErrorCode() == ErrorCode.SUCCESS) {
        logInfo("Created partition `{0}` in collection `{1}` successfully!", tag, collectionName);
        return new Response(Response.Status.SUCCESS);
      } else {
        logSevere(
            "Create partition `{0}` in collection `{1}` failed: {2}",
            tag, collectionName, response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logSevere("createPartition RPC failed:\n{0}", e.getStatus().toString());
      return new Response(Response.Status.RPC_ERROR, e.toString());
    }
  }

  @Override
  public HasPartitionResponse hasPartition(String collectionName, String tag) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new HasPartitionResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED), false);
    }

    PartitionParam request =
        PartitionParam.newBuilder().setCollectionName(collectionName).setTag(tag).build();
    BoolReply response;

    try {
      response = blockingStub.hasPartition(request);

      if (response.getStatus().getErrorCode() == ErrorCode.SUCCESS) {
        logInfo("hasPartition with tag `{0}` in `{1}` = {2}", tag, collectionName, response.getBoolReply());
        return new HasPartitionResponse(
                new Response(Response.Status.SUCCESS), response.getBoolReply());
      } else {
        logSevere("hasPartition with tag `{0}` in `{1}` failed:\n{2}", tag, collectionName, response.toString());
        return new HasPartitionResponse(
                new Response(
                        Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                        response.getStatus().getReason()),
                false);
      }
    } catch (StatusRuntimeException e) {
      logSevere("hasPartition RPC failed:\n{0}", e.getStatus().toString());
      return new HasPartitionResponse(
              new Response(Response.Status.RPC_ERROR, e.toString()), false);
    }
  }

  @Override
  public ShowPartitionsResponse showPartitions(String collectionName) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new ShowPartitionsResponse(
          new Response(Response.Status.CLIENT_NOT_CONNECTED), new ArrayList<>());
    }

    CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();
    PartitionList response;

    try {
      response = blockingStub.showPartitions(request);

      if (response.getStatus().getErrorCode() == ErrorCode.SUCCESS) {
        logInfo(
            "Current partitions of collection {0}: {1}",
            collectionName, response.getPartitionTagArrayList());
        return new ShowPartitionsResponse(
            new Response(Response.Status.SUCCESS), response.getPartitionTagArrayList());
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
  public Response dropPartition(String collectionName, String tag) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    PartitionParam request =
        PartitionParam.newBuilder().setCollectionName(collectionName).setTag(tag).build();
    Status response;

    try {
      response = blockingStub.dropPartition(request);

      if (response.getErrorCode() == ErrorCode.SUCCESS) {
        logInfo("Dropped partition `{1}` in collection `{1}` successfully!", tag, collectionName);
        return new Response(Response.Status.SUCCESS);
      } else {
        logSevere(
            "Drop partition `{0}` in collection `{1}` failed:\n{1}",
            tag, collectionName, response.toString());
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

    List<RowRecord> rowRecordList =
        buildRowRecordList(insertParam.getFloatVectors(), insertParam.getBinaryVectors());

    io.milvus.grpc.InsertParam request =
        io.milvus.grpc.InsertParam.newBuilder()
            .setCollectionName(insertParam.getCollectionName())
            .addAllRowRecordArray(rowRecordList)
            .addAllRowIdArray(insertParam.getVectorIds())
            .setPartitionTag(insertParam.getPartitionTag())
            .build();
    VectorIds response;

    try {
      response = blockingStub.insert(request);

      if (response.getStatus().getErrorCode() == ErrorCode.SUCCESS) {
        logInfo(
            "Inserted {0} vectors to collection `{1}` successfully!",
            response.getVectorIdArrayCount(), insertParam.getCollectionName());
        return new InsertResponse(
            new Response(Response.Status.SUCCESS), response.getVectorIdArrayList());
      } else {
        logSevere("Insert vectors failed:\n{0}", response.getStatus().toString());
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
  public ListenableFuture<InsertResponse> insertAsync(@Nonnull InsertParam insertParam) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return Futures.immediateFuture(
          new InsertResponse(
              new Response(Response.Status.CLIENT_NOT_CONNECTED), new ArrayList<>()));
    }

    List<RowRecord> rowRecordList =
        buildRowRecordList(insertParam.getFloatVectors(), insertParam.getBinaryVectors());

    io.milvus.grpc.InsertParam request =
        io.milvus.grpc.InsertParam.newBuilder()
            .setCollectionName(insertParam.getCollectionName())
            .addAllRowRecordArray(rowRecordList)
            .addAllRowIdArray(insertParam.getVectorIds())
            .setPartitionTag(insertParam.getPartitionTag())
            .build();

    ListenableFuture<VectorIds> response;

    response = futureStub.insert(request);

    Futures.addCallback(
        response,
        new FutureCallback<VectorIds>() {
          @Override
          public void onSuccess(VectorIds result) {
            if (result.getStatus().getErrorCode() == ErrorCode.SUCCESS) {
              logInfo(
                  "Inserted {0} vectors to collection `{1}` successfully!",
                  result.getVectorIdArrayCount(), insertParam.getCollectionName());
            } else {
              logSevere("InsertAsync failed:\n{0}", result.getStatus().toString());
            }
          }

          @Override
          public void onFailure(Throwable t) {
            logSevere("InsertAsync failed:\n{0}", t.getMessage());
          }
        },
        MoreExecutors.directExecutor());

    Function<VectorIds, InsertResponse> transformFunc =
        vectorIds -> {
          if (vectorIds.getStatus().getErrorCode() == ErrorCode.SUCCESS) {
            return new InsertResponse(
                new Response(Response.Status.SUCCESS), vectorIds.getVectorIdArrayList());
          } else {
            return new InsertResponse(
                new Response(
                    Response.Status.valueOf(vectorIds.getStatus().getErrorCodeValue()),
                    vectorIds.getStatus().getReason()),
                new ArrayList<>());
          }
        };

    return Futures.transform(response, transformFunc::apply, MoreExecutors.directExecutor());
  }

  @Override
  public SearchResponse search(@Nonnull SearchParam searchParam) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      SearchResponse searchResponse = new SearchResponse();
      searchResponse.setResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED));
      return searchResponse;
    }

    List<RowRecord> rowRecordList =
        buildRowRecordList(searchParam.getFloatVectors(), searchParam.getBinaryVectors());

    KeyValuePair extraParam =
        KeyValuePair.newBuilder()
            .setKey(extraParamKey)
            .setValue(searchParam.getParamsInJson())
            .build();

    io.milvus.grpc.SearchParam request =
        io.milvus.grpc.SearchParam.newBuilder()
            .setCollectionName(searchParam.getCollectionName())
            .addAllQueryRecordArray(rowRecordList)
            .addAllPartitionTagArray(searchParam.getPartitionTags())
            .setTopk(searchParam.getTopK())
            .addExtraParams(extraParam)
            .build();

    TopKQueryResult response;

    try {
      response = blockingStub.search(request);

      if (response.getStatus().getErrorCode() == ErrorCode.SUCCESS) {
        SearchResponse searchResponse = buildSearchResponse(response);
        searchResponse.setResponse(new Response(Response.Status.SUCCESS));
        logInfo(
            "Search completed successfully! Returned results for {0} queries",
            searchResponse.getNumQueries());
        return searchResponse;
      } else {
        logSevere("Search failed:\n{0}", response.getStatus().toString());
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
  public SearchResponse searchByIds(@Nonnull SearchByIdsParam searchByIdsParam) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      SearchResponse searchResponse = new SearchResponse();
      searchResponse.setResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED));
      return searchResponse;
    }

    List<Long> idList = searchByIdsParam.getIds();

    KeyValuePair extraParam =
            KeyValuePair.newBuilder()
                    .setKey(extraParamKey)
                    .setValue(searchByIdsParam.getParamsInJson())
                    .build();

    io.milvus.grpc.SearchByIDParam request =
            io.milvus.grpc.SearchByIDParam.newBuilder()
                    .setCollectionName(searchByIdsParam.getCollectionName())
                    .addAllIdArray(idList)
                    .addAllPartitionTagArray(searchByIdsParam.getPartitionTags())
                    .setTopk(searchByIdsParam.getTopK())
                    .addExtraParams(extraParam)
                    .build();

    TopKQueryResult response;

    try {
      response = blockingStub.searchByID(request);

      if (response.getStatus().getErrorCode() == ErrorCode.SUCCESS) {
        SearchResponse searchResponse = buildSearchResponse(response);
        searchResponse.setResponse(new Response(Response.Status.SUCCESS));
        logInfo(
                "Search completed successfully! Returned results for {0} queries",
                searchResponse.getNumQueries());
        return searchResponse;
      } else {
        logSevere("Search failed:\n{0}", response.getStatus().toString());
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
  public ListenableFuture<SearchResponse> searchAsync(@Nonnull SearchParam searchParam) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      SearchResponse searchResponse = new SearchResponse();
      searchResponse.setResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED));
      return Futures.immediateFuture(searchResponse);
    }

    List<RowRecord> rowRecordList =
        buildRowRecordList(searchParam.getFloatVectors(), searchParam.getBinaryVectors());

    KeyValuePair extraParam =
        KeyValuePair.newBuilder()
            .setKey(extraParamKey)
            .setValue(searchParam.getParamsInJson())
            .build();

    io.milvus.grpc.SearchParam request =
        io.milvus.grpc.SearchParam.newBuilder()
            .setCollectionName(searchParam.getCollectionName())
            .addAllQueryRecordArray(rowRecordList)
            .addAllPartitionTagArray(searchParam.getPartitionTags())
            .setTopk(searchParam.getTopK())
            .addExtraParams(extraParam)
            .build();

    ListenableFuture<TopKQueryResult> response;

    response = futureStub.search(request);

    Futures.addCallback(
        response,
        new FutureCallback<TopKQueryResult>() {
          @Override
          public void onSuccess(TopKQueryResult result) {
            if (result.getStatus().getErrorCode() == ErrorCode.SUCCESS) {
              logInfo(
                  "SearchAsync completed successfully! Returned results for {0} queries",
                  result.getRowNum());
            } else {
              logSevere("SearchAsync failed:\n{0}", result.getStatus().toString());
            }
          }

          @Override
          public void onFailure(Throwable t) {
            logSevere("SearchAsync failed:\n{0}", t.getMessage());
          }
        },
        MoreExecutors.directExecutor());

    Function<TopKQueryResult, SearchResponse> transformFunc =
        topKQueryResult -> {
          if (topKQueryResult.getStatus().getErrorCode() == ErrorCode.SUCCESS) {
            SearchResponse searchResponse = buildSearchResponse(topKQueryResult);
            searchResponse.setResponse(new Response(Response.Status.SUCCESS));
            return searchResponse;
          } else {
            SearchResponse searchResponse = new SearchResponse();
            searchResponse.setResponse(
                new Response(
                    Response.Status.valueOf(topKQueryResult.getStatus().getErrorCodeValue()),
                    topKQueryResult.getStatus().getReason()));
            return searchResponse;
          }
        };

    return Futures.transform(response, transformFunc::apply, MoreExecutors.directExecutor());
  }

  @Override
  public SearchResponse searchInFiles(
      @Nonnull List<String> fileIds, @Nonnull SearchParam searchParam) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      SearchResponse searchResponse = new SearchResponse();
      searchResponse.setResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED));
      return searchResponse;
    }

    List<RowRecord> rowRecordList =
        buildRowRecordList(searchParam.getFloatVectors(), searchParam.getBinaryVectors());

    KeyValuePair extraParam =
        KeyValuePair.newBuilder()
            .setKey(extraParamKey)
            .setValue(searchParam.getParamsInJson())
            .build();

    io.milvus.grpc.SearchParam constructSearchParam =
        io.milvus.grpc.SearchParam.newBuilder()
            .setCollectionName(searchParam.getCollectionName())
            .addAllQueryRecordArray(rowRecordList)
            .addAllPartitionTagArray(searchParam.getPartitionTags())
            .setTopk(searchParam.getTopK())
            .addExtraParams(extraParam)
            .build();

    SearchInFilesParam request =
        SearchInFilesParam.newBuilder()
            .addAllFileIdArray(fileIds)
            .setSearchParam(constructSearchParam)
            .build();

    TopKQueryResult response;

    try {
      response = blockingStub.searchInFiles(request);

      if (response.getStatus().getErrorCode() == ErrorCode.SUCCESS) {
        SearchResponse searchResponse = buildSearchResponse(response);
        searchResponse.setResponse(new Response(Response.Status.SUCCESS));
        logInfo(
            "Search in files completed successfully! Returned results for {0} queries",
            searchResponse.getNumQueries());
        return searchResponse;
      } else {
        logSevere("Search in files failed: {0}", response.getStatus().toString());

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
  public DescribeCollectionResponse describeCollection(@Nonnull String collectionName) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new DescribeCollectionResponse(
          new Response(Response.Status.CLIENT_NOT_CONNECTED), null);
    }

    CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();
    CollectionSchema response;

    try {
      response = blockingStub.describeCollection(request);

      if (response.getStatus().getErrorCode() == ErrorCode.SUCCESS) {
        CollectionMapping collectionMapping =
            new CollectionMapping.Builder(response.getCollectionName(), response.getDimension())
                .withIndexFileSize(response.getIndexFileSize())
                .withMetricType(MetricType.valueOf(response.getMetricType()))
                .build();
        logInfo("Describe Collection `{0}` returned:\n{1}", collectionName, collectionMapping);
        return new DescribeCollectionResponse(
            new Response(Response.Status.SUCCESS), collectionMapping);
      } else {
        logSevere(
            "Describe Collection `{0}` failed:\n{1}",
            collectionName, response.getStatus().toString());
        return new DescribeCollectionResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()),
            null);
      }
    } catch (StatusRuntimeException e) {
      logSevere("describeCollection RPC failed:\n{0}", e.getStatus().toString());
      return new DescribeCollectionResponse(
          new Response(Response.Status.RPC_ERROR, e.toString()), null);
    }
  }

  @Override
  public ShowCollectionsResponse showCollections() {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new ShowCollectionsResponse(
          new Response(Response.Status.CLIENT_NOT_CONNECTED), new ArrayList<>());
    }

    Command request = Command.newBuilder().setCmd("").build();
    CollectionNameList response;

    try {
      response = blockingStub.showCollections(request);

      if (response.getStatus().getErrorCode() == ErrorCode.SUCCESS) {
        List<String> collectionNames = response.getCollectionNamesList();
        logInfo("Current collections: {0}", collectionNames.toString());
        return new ShowCollectionsResponse(new Response(Response.Status.SUCCESS), collectionNames);
      } else {
        logSevere("Show collections failed:\n{0}", response.getStatus().toString());
        return new ShowCollectionsResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()),
            new ArrayList<>());
      }
    } catch (StatusRuntimeException e) {
      logSevere("showCollections RPC failed:\n{0}", e.getStatus().toString());
      return new ShowCollectionsResponse(
          new Response(Response.Status.RPC_ERROR, e.toString()), new ArrayList<>());
    }
  }

  @Override
  public GetCollectionRowCountResponse getCollectionRowCount(@Nonnull String collectionName) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new GetCollectionRowCountResponse(
          new Response(Response.Status.CLIENT_NOT_CONNECTED), 0);
    }

    CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();
    CollectionRowCount response;

    try {
      response = blockingStub.countCollection(request);

      if (response.getStatus().getErrorCode() == ErrorCode.SUCCESS) {
        long collectionRowCount = response.getCollectionRowCount();
        logInfo("Collection `{0}` has {1} rows", collectionName, collectionRowCount);
        return new GetCollectionRowCountResponse(
            new Response(Response.Status.SUCCESS), collectionRowCount);
      } else {
        logSevere(
            "Get collection `{0}` row count failed:\n{1}",
            collectionName, response.getStatus().toString());
        return new GetCollectionRowCountResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()),
            0);
      }
    } catch (StatusRuntimeException e) {
      logSevere("countCollection RPC failed:\n{0}", e.getStatus().toString());
      return new GetCollectionRowCountResponse(
          new Response(Response.Status.RPC_ERROR, e.toString()), 0);
    }
  }

  @Override
  public Response getServerStatus() {
    return command("status");
  }

  @Override
  public Response getServerVersion() {
    return command("version");
  }

  public Response command(@Nonnull String command) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    Command request = Command.newBuilder().setCmd(command).build();
    StringReply response;

    try {
      response = blockingStub.cmd(request);

      if (response.getStatus().getErrorCode() == ErrorCode.SUCCESS) {
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

  @Override
  public Response preloadCollection(@Nonnull String collectionName) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();
    Status response;

    try {
      response = blockingStub.preloadCollection(request);

      if (response.getErrorCode() == ErrorCode.SUCCESS) {
        logInfo("Preloaded collection `{0}` successfully!", collectionName);
        return new Response(Response.Status.SUCCESS);
      } else {
        logSevere("Preload collection `{0}` failed:\n{1}", collectionName, response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logSevere("preloadCollection RPC failed:\n{0}", e.getStatus().toString());
      return new Response(Response.Status.RPC_ERROR, e.toString());
    }
  }

  @Override
  public DescribeIndexResponse describeIndex(@Nonnull String collectionName) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new DescribeIndexResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED), null);
    }

    CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();
    IndexParam response;

    try {
      response = blockingStub.describeIndex(request);

      if (response.getStatus().getErrorCode() == ErrorCode.SUCCESS) {
        String extraParam = "";
        for (KeyValuePair kv : response.getExtraParamsList()) {
          if (kv.getKey().contentEquals(extraParamKey)) {
            extraParam = kv.getValue();
          }
        }
        Index index =
            new Index.Builder(response.getCollectionName(), IndexType.valueOf(response.getIndexType()))
                .withParamsInJson(extraParam)
                .build();
        logInfo(
            "Describe index for collection `{0}` returned:\n{1}", collectionName, index.toString());
        return new DescribeIndexResponse(new Response(Response.Status.SUCCESS), index);
      } else {
        logSevere(
            "Describe index for collection `{0}` failed:\n{1}",
            collectionName, response.getStatus().toString());
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
  public Response dropIndex(@Nonnull String collectionName) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();
    Status response;

    try {
      response = blockingStub.dropIndex(request);

      if (response.getErrorCode() == ErrorCode.SUCCESS) {
        logInfo("Dropped index for collection `{0}` successfully!", collectionName);
        return new Response(Response.Status.SUCCESS);
      } else {
        logSevere(
            "Drop index for collection `{0}` failed:\n{1}", collectionName, response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logSevere("dropIndex RPC failed:\n{0}", e.getStatus().toString());
      return new Response(Response.Status.RPC_ERROR, e.toString());
    }
  }

  @Override
  public Response showCollectionInfo(String collectionName) {
    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();
    io.milvus.grpc.CollectionInfo response;

    try {
      response = blockingStub.showCollectionInfo(request);

      if (response.getStatus().getErrorCode() == ErrorCode.SUCCESS) {
        logInfo("ShowCollectionInfo for `{0}` returned successfully!", collectionName);
        return new Response(Response.Status.SUCCESS, response.getJsonInfo());
      } else {
        logSevere(
            "ShowCollectionInfo for `{0}` failed:\n{1}",
            collectionName, response.getStatus().toString());
        return new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason());
      }
    } catch (StatusRuntimeException e) {
      logSevere("showCollectionInfo RPC failed:\n{0}", e.getStatus().toString());
      return new Response(Response.Status.RPC_ERROR, e.toString());
    }
  }

  @Override
  public GetVectorsByIdsResponse getVectorsByIds(String collectionName, List<Long> ids) {
    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new GetVectorsByIdsResponse(
              new Response(Response.Status.CLIENT_NOT_CONNECTED), new ArrayList<>(), null);
    }

    VectorsIdentity request =
        VectorsIdentity.newBuilder().setCollectionName(collectionName).addAllIdArray(ids).build();
    VectorsData response;

    try {
      response = blockingStub.getVectorsByID(request);

      if (response.getStatus().getErrorCode() == ErrorCode.SUCCESS) {

        logInfo(
            "getVectorsByIds in collection `{0}` returned successfully!", collectionName);

        List<List<Float>> floatVectors = new ArrayList<>();
        List<ByteBuffer> binaryVectors = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) {
          floatVectors.add(response.getVectorsData(i).getFloatDataList());
          binaryVectors.add(response.getVectorsData(i).getBinaryData().asReadOnlyByteBuffer());
        }
        return new GetVectorsByIdsResponse(
                new Response(Response.Status.SUCCESS), floatVectors, binaryVectors);

      } else {
        logSevere(
            "getVectorsByIds in collection `{0}` failed:\n{1}",
            collectionName, response.getStatus().toString());
        return new GetVectorsByIdsResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()),
            new ArrayList<>(),
            null);
      }
    } catch (StatusRuntimeException e) {
      logSevere("getVectorsByIds RPC failed:\n{0}", e.getStatus().toString());
      return new GetVectorsByIdsResponse(
          new Response(Response.Status.RPC_ERROR, e.toString()), new ArrayList<>(), null);
    }
  }

  @Override
  public GetVectorIdsResponse getVectorIds(String collectionName, String segmentName) {
    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new GetVectorIdsResponse(
          new Response(Response.Status.CLIENT_NOT_CONNECTED), new ArrayList<>());
    }

    GetVectorIDsParam request =
        GetVectorIDsParam.newBuilder()
            .setCollectionName(collectionName)
            .setSegmentName(segmentName)
            .build();
    VectorIds response;

    try {
      response = blockingStub.getVectorIDs(request);

      if (response.getStatus().getErrorCode() == ErrorCode.SUCCESS) {

        logInfo(
            "getVectorIds in collection `{0}`, segment `{1}` returned successfully!",
            collectionName, segmentName);
        return new GetVectorIdsResponse(
            new Response(Response.Status.SUCCESS), response.getVectorIdArrayList());
      } else {
        logSevere(
            "getVectorIds in collection `{0}`, segment `{1}` failed:\n{2}",
            collectionName, segmentName, response.getStatus().toString());
        return new GetVectorIdsResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()),
            new ArrayList<>());
      }
    } catch (StatusRuntimeException e) {
      logSevere("getVectorIds RPC failed:\n{0}", e.getStatus().toString());
      return new GetVectorIdsResponse(
          new Response(Response.Status.RPC_ERROR, e.toString()), new ArrayList<>());
    }
  }

  @Override
  public Response deleteByIds(String collectionName, List<Long> ids) {
    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    DeleteByIDParam request =
        DeleteByIDParam.newBuilder().setCollectionName(collectionName).addAllIdArray(ids).build();
    Status response;

    try {
      response = blockingStub.deleteByID(request);

      if (response.getErrorCode() == ErrorCode.SUCCESS) {
        logInfo("deleteByIds in collection `{0}` completed successfully!", collectionName);
        return new Response(Response.Status.SUCCESS);
      } else {
        logSevere(
            "deleteByIds in collection `{0}` failed:\n{1}", collectionName, response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logSevere("deleteByIds RPC failed:\n{0}", e.getStatus().toString());
      return new Response(Response.Status.RPC_ERROR, e.toString());
    }
  }

  @Override
  public Response deleteById(String collectionName, Long id) {
    List<Long> list =
        new ArrayList<Long>() {
          {
            add(id);
          }
        };
    return deleteByIds(collectionName, list);
  }

  @Override
  public Response flush(List<String> collectionNames) {
    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    FlushParam request = FlushParam.newBuilder().addAllCollectionNameArray(collectionNames).build();
    Status response;

    try {
      response = blockingStub.flush(request);

      if (response.getErrorCode() == ErrorCode.SUCCESS) {
        logInfo("Flushed collection {0} successfully!", collectionNames);
        return new Response(Response.Status.SUCCESS);
      } else {
        logSevere("Flush collection {0} failed:\n{1}", collectionNames, response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logSevere("flush RPC failed:\n{0}", e.getStatus().toString());
      return new Response(Response.Status.RPC_ERROR, e.toString());
    }
  }

  @Override
  public ListenableFuture<Response> flushAsync(@Nonnull List<String> collectionNames) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return Futures.immediateFuture(new Response(Response.Status.CLIENT_NOT_CONNECTED));
    }

    FlushParam request = FlushParam.newBuilder().addAllCollectionNameArray(collectionNames).build();

    ListenableFuture<Status> response;

    response = futureStub.flush(request);

    Futures.addCallback(
        response,
        new FutureCallback<Status>() {
          @Override
          public void onSuccess(Status result) {
            if (result.getErrorCode() == ErrorCode.SUCCESS) {
              logInfo("Flushed collection {0} successfully!", collectionNames);
            } else {
              logSevere("Flush collection {0} failed:\n{1}", collectionNames, result.toString());
            }
          }

          @Override
          public void onFailure(Throwable t) {
            logSevere("FlushAsync failed:\n{0}", t.getMessage());
          }
        },
        MoreExecutors.directExecutor());

    return Futures.transform(
        response, transformStatusToResponseFunc::apply, MoreExecutors.directExecutor());
  }

  @Override
  public Response flush(String collectionName) {
    List<String> list =
        new ArrayList<String>() {
          {
            add(collectionName);
          }
        };
    return flush(list);
  }

  @Override
  public ListenableFuture<Response> flushAsync(String collectionName) {
    List<String> list =
        new ArrayList<String>() {
          {
            add(collectionName);
          }
        };
    return flushAsync(list);
  }

  @Override
  public Response compact(String collectionName) {
    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();
    Status response;

    try {
      response = blockingStub.compact(request);

      if (response.getErrorCode() == ErrorCode.SUCCESS) {
        logInfo("Compacted collection `{0}` successfully!", collectionName);
        return new Response(Response.Status.SUCCESS);
      } else {
        logSevere("Compact collection `{0}` failed:\n{1}", collectionName, response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logSevere("compact RPC failed:\n{0}", e.getStatus().toString());
      return new Response(Response.Status.RPC_ERROR, e.toString());
    }
  }

  @Override
  public ListenableFuture<Response> compactAsync(@Nonnull String collectionName) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return Futures.immediateFuture(new Response(Response.Status.CLIENT_NOT_CONNECTED));
    }

    CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();

    ListenableFuture<Status> response;

    response = futureStub.compact(request);

    Futures.addCallback(
        response,
        new FutureCallback<Status>() {
          @Override
          public void onSuccess(Status result) {
            if (result.getErrorCode() == ErrorCode.SUCCESS) {
              logInfo("Compacted collection `{0}` successfully!", collectionName);
            } else {
              logSevere("Compact collection `{0}` failed:\n{1}", collectionName, result.toString());
            }
          }

          @Override
          public void onFailure(Throwable t) {
            logSevere("CompactAsync failed:\n{0}", t.getMessage());
          }
        },
        MoreExecutors.directExecutor());

    return Futures.transform(
        response, transformStatusToResponseFunc::apply, MoreExecutors.directExecutor());
  }

  ///////////////////// Util Functions/////////////////////
  Function<Status, Response> transformStatusToResponseFunc =
      status -> {
        if (status.getErrorCode() == ErrorCode.SUCCESS) {
          return new Response(Response.Status.SUCCESS);
        } else {
          return new Response(
              Response.Status.valueOf(status.getErrorCodeValue()), status.getReason());
        }
      };

  private List<RowRecord> buildRowRecordList(
      @Nonnull List<List<Float>> floatVectors, @Nonnull List<ByteBuffer> binaryVectors) {
    List<RowRecord> rowRecordList = new ArrayList<>();

    int largerSize = Math.max(floatVectors.size(), binaryVectors.size());

    for (int i = 0; i < largerSize; ++i) {

      RowRecord.Builder rowRecordBuilder = RowRecord.newBuilder();

      if (i < floatVectors.size()) {
        rowRecordBuilder.addAllFloatData(floatVectors.get(i));
      }
      if (i < binaryVectors.size()) {
        ((Buffer) binaryVectors.get(i)).rewind();
        rowRecordBuilder.setBinaryData(ByteString.copyFrom(binaryVectors.get(i)));
      }

      rowRecordList.add(rowRecordBuilder.build());
    }

    return rowRecordList;
  }

  private SearchResponse buildSearchResponse(TopKQueryResult topKQueryResult) {

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
