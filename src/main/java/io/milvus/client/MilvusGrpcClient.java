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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/** Actual implementation of interface <code>MilvusClient</code> */
public class MilvusGrpcClient implements MilvusClient {

  private static final Logger logger = LoggerFactory.getLogger(MilvusGrpcClient.class);
  private final String extraParamKey = "params";
  private ManagedChannel channel = null;
  private MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub = null;
  private MilvusServiceGrpc.MilvusServiceFutureStub futureStub = null;

  ////////////////////// Constructor //////////////////////
  public MilvusGrpcClient() {}

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
      logInfo("Trying to connect...Timeout in {} ms", timeout);

      final long checkFrequency = 100; // ms
      while (channel.getState(false) != ConnectivityState.READY) {
        if (timeout <= 0) {
          logError("Connect timeout!");
          throw new ConnectFailedException("Connect timeout");
        }
        TimeUnit.MILLISECONDS.sleep(checkFrequency);
        timeout -= checkFrequency;
      }

      blockingStub = MilvusServiceGrpc.newBlockingStub(channel);
      futureStub = MilvusServiceGrpc.newFutureStub(channel);

      // check server version
      String serverVersion = getServerVersion().getMessage();
      if (!serverVersion.contains("0.10.")) {
        logError(
            "Connect failed! Server version {} does not match SDK version 0.8.2", serverVersion);
        throw new ConnectFailedException("Failed to connect to Milvus server.");
      }

    } catch (Exception e) {
      if (!(e instanceof ConnectFailedException)) {
        logError("Connect failed! {}", e.toString());
      }
      throw new ConnectFailedException("Exception occurred: " + e.toString());
    }

    logInfo(
        "Connection established successfully to host={}, port={}",
        connectParam.getHost(),
        String.valueOf(connectParam.getPort()));
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
          logError("Encountered error when terminating channel");
          return new Response(Response.Status.RPC_ERROR);
        }
      } catch (InterruptedException e) {
        logError("Exception thrown when terminating channel: {}", e.toString());
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
        logInfo("Created collection successfully!\n{}", collectionMapping.toString());
        return new Response(Response.Status.SUCCESS);
      } else if (response.getReason().contentEquals("Collection already exists")) {
        logWarning("Collection `{}` already exists", collectionMapping.getCollectionName());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      } else {
        logError(
            "Create collection failed\n{}\n{}", collectionMapping.toString(), response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logError("createCollection RPC failed:\n{}", e.getStatus().toString());
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
        logInfo("hasCollection `{}` = {}", collectionName, response.getBoolReply());
        return new HasCollectionResponse(
            new Response(Response.Status.SUCCESS), response.getBoolReply());
      } else {
        logError("hasCollection `{}` failed:\n{}", collectionName, response.toString());
        return new HasCollectionResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()),
            false);
      }
    } catch (StatusRuntimeException e) {
      logError("hasCollection RPC failed:\n{}", e.getStatus().toString());
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
        logInfo("Dropped collection `{}` successfully!", collectionName);
        return new Response(Response.Status.SUCCESS);
      } else {
        logError("Drop collection `{}` failed:\n{}", collectionName, response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logError("dropCollection RPC failed:\n{}", e.getStatus().toString());
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
        logInfo("Created index successfully!\n{}", index.toString());
        return new Response(Response.Status.SUCCESS);
      } else {
        logError("Create index failed:\n{}\n{}", index.toString(), response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logError("createIndex RPC failed:\n{}", e.getStatus().toString());
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
              logInfo("Created index successfully!\n{}", index.toString());
            } else {
              logError("CreateIndexAsync failed:\n{}\n{}", index.toString(), result.toString());
            }
          }

          @Override
          public void onFailure(Throwable t) {
            logError("CreateIndexAsync failed:\n{}", t.getMessage());
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
        logInfo("Created partition `{}` in collection `{}` successfully!", tag, collectionName);
        return new Response(Response.Status.SUCCESS);
      } else {
        logError(
            "Create partition `{}` in collection `{}` failed: {}",
            tag,
            collectionName,
            response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logError("createPartition RPC failed:\n{}", e.getStatus().toString());
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
        logInfo(
            "hasPartition with tag `{}` in `{}` = {}",
            tag,
            collectionName,
            response.getBoolReply());
        return new HasPartitionResponse(
            new Response(Response.Status.SUCCESS), response.getBoolReply());
      } else {
        logError(
            "hasPartition with tag `{}` in `{}` failed:\n{}",
            tag,
            collectionName,
            response.toString());
        return new HasPartitionResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()),
            false);
      }
    } catch (StatusRuntimeException e) {
      logError("hasPartition RPC failed:\n{}", e.getStatus().toString());
      return new HasPartitionResponse(new Response(Response.Status.RPC_ERROR, e.toString()), false);
    }
  }

  @Override
  public ListPartitionsResponse listPartitions(String collectionName) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new ListPartitionsResponse(
          new Response(Response.Status.CLIENT_NOT_CONNECTED), new ArrayList<>());
    }

    CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();
    PartitionList response;

    try {
      response = blockingStub.showPartitions(request);

      if (response.getStatus().getErrorCode() == ErrorCode.SUCCESS) {
        logInfo(
            "Current partitions of collection {}: {}",
            collectionName,
            response.getPartitionTagArrayList());
        return new ListPartitionsResponse(
            new Response(Response.Status.SUCCESS), response.getPartitionTagArrayList());
      } else {
        logError("List partitions failed:\n{}", response.toString());
        return new ListPartitionsResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()),
            new ArrayList<>());
      }
    } catch (StatusRuntimeException e) {
      logError("listPartitions RPC failed:\n{}", e.getStatus().toString());
      return new ListPartitionsResponse(
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
        logInfo("Dropped partition `{}` in collection `{}` successfully!", tag, collectionName);
        return new Response(Response.Status.SUCCESS);
      } else {
        logError(
            "Drop partition `{}` in collection `{}` failed:\n{}",
            tag,
            collectionName,
            response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logError("dropPartition RPC failed:\n{}", e.getStatus().toString());
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
            "Inserted {} vectors to collection `{}` successfully!",
            response.getVectorIdArrayCount(),
            insertParam.getCollectionName());
        return new InsertResponse(
            new Response(Response.Status.SUCCESS), response.getVectorIdArrayList());
      } else {
        logError("Insert vectors failed:\n{}", response.getStatus().toString());
        return new InsertResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()),
            new ArrayList<>());
      }
    } catch (StatusRuntimeException e) {
      logError("insert RPC failed:\n{}", e.getStatus().toString());
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
                  "Inserted {} vectors to collection `{}` successfully!",
                  result.getVectorIdArrayCount(),
                  insertParam.getCollectionName());
            } else {
              logError("InsertAsync failed:\n{}", result.getStatus().toString());
            }
          }

          @Override
          public void onFailure(Throwable t) {
            logError("InsertAsync failed:\n{}", t.getMessage());
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
            "Search completed successfully! Returned results for {} queries",
            searchResponse.getNumQueries());
        return searchResponse;
      } else {
        logError("Search failed:\n{}", response.getStatus().toString());
        SearchResponse searchResponse = new SearchResponse();
        searchResponse.setResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()));
        return searchResponse;
      }
    } catch (StatusRuntimeException e) {
      logError("search RPC failed:\n{}", e.getStatus().toString());
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
                  "SearchAsync completed successfully! Returned results for {} queries",
                  result.getRowNum());
            } else {
              logError("SearchAsync failed:\n{}", result.getStatus().toString());
            }
          }

          @Override
          public void onFailure(Throwable t) {
            logError("SearchAsync failed:\n{}", t.getMessage());
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
  public GetCollectionInfoResponse getCollectionInfo(@Nonnull String collectionName) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new GetCollectionInfoResponse(
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
        logInfo("Get Collection Info `{}` returned:\n{}", collectionName, collectionMapping);
        return new GetCollectionInfoResponse(
            new Response(Response.Status.SUCCESS), collectionMapping);
      } else {
        logError(
            "Get Collection Info `{}` failed:\n{}",
            collectionName,
            response.getStatus().toString());
        return new GetCollectionInfoResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()),
            null);
      }
    } catch (StatusRuntimeException e) {
      logError("getCollectionInfo RPC failed:\n{}", e.getStatus().toString());
      return new GetCollectionInfoResponse(
          new Response(Response.Status.RPC_ERROR, e.toString()), null);
    }
  }

  @Override
  public ListCollectionsResponse listCollections() {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new ListCollectionsResponse(
          new Response(Response.Status.CLIENT_NOT_CONNECTED), new ArrayList<>());
    }

    Command request = Command.newBuilder().setCmd("").build();
    CollectionNameList response;

    try {
      response = blockingStub.showCollections(request);

      if (response.getStatus().getErrorCode() == ErrorCode.SUCCESS) {
        List<String> collectionNames = response.getCollectionNamesList();
        logInfo("Current collections: {}", collectionNames.toString());
        return new ListCollectionsResponse(new Response(Response.Status.SUCCESS), collectionNames);
      } else {
        logError("List collections failed:\n{}", response.getStatus().toString());
        return new ListCollectionsResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()),
            new ArrayList<>());
      }
    } catch (StatusRuntimeException e) {
      logError("listCollections RPC failed:\n{}", e.getStatus().toString());
      return new ListCollectionsResponse(
          new Response(Response.Status.RPC_ERROR, e.toString()), new ArrayList<>());
    }
  }

  @Override
  public CountEntitiesResponse countEntities(@Nonnull String collectionName) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new CountEntitiesResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED), 0);
    }

    CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();
    CollectionRowCount response;

    try {
      response = blockingStub.countCollection(request);

      if (response.getStatus().getErrorCode() == ErrorCode.SUCCESS) {
        long collectionRowCount = response.getCollectionRowCount();
        logInfo("Collection `{}` has {} entities", collectionName, collectionRowCount);
        return new CountEntitiesResponse(new Response(Response.Status.SUCCESS), collectionRowCount);
      } else {
        logError(
            "Get collection `{}` entity count failed:\n{}",
            collectionName,
            response.getStatus().toString());
        return new CountEntitiesResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()),
            0);
      }
    } catch (StatusRuntimeException e) {
      logError("countEntities RPC failed:\n{}", e.getStatus().toString());
      return new CountEntitiesResponse(new Response(Response.Status.RPC_ERROR, e.toString()), 0);
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
        logInfo("Command `{}`: {}", command, response.getStringReply());
        return new Response(Response.Status.SUCCESS, response.getStringReply());
      } else {
        logError("Command `{}` failed:\n{}", command, response.toString());
        return new Response(
            Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
            response.getStatus().getReason());
      }
    } catch (StatusRuntimeException e) {
      logError("Command RPC failed:\n{}", e.getStatus().toString());
      return new Response(Response.Status.RPC_ERROR, e.toString());
    }
  }

  @Override
  public Response loadCollection(@Nonnull String collectionName) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();
    Status response;

    try {
      response = blockingStub.preloadCollection(request);

      if (response.getErrorCode() == ErrorCode.SUCCESS) {
        logInfo("Loaded collection `{}` successfully!", collectionName);
        return new Response(Response.Status.SUCCESS);
      } else {
        logError("Load collection `{}` failed:\n{}", collectionName, response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logError("loadCollection RPC failed:\n{}", e.getStatus().toString());
      return new Response(Response.Status.RPC_ERROR, e.toString());
    }
  }

  @Override
  public GetIndexInfoResponse getIndexInfo(@Nonnull String collectionName) {

    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new GetIndexInfoResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED), null);
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
            new Index.Builder(
                    response.getCollectionName(), IndexType.valueOf(response.getIndexType()))
                .withParamsInJson(extraParam)
                .build();
        logInfo(
            "Get index info for collection `{}` returned:\n{}", collectionName, index.toString());
        return new GetIndexInfoResponse(new Response(Response.Status.SUCCESS), index);
      } else {
        logError(
            "Get index info for collection `{}` failed:\n{}",
            collectionName,
            response.getStatus().toString());
        return new GetIndexInfoResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()),
            null);
      }
    } catch (StatusRuntimeException e) {
      logError("getIndexInfo RPC failed:\n{}", e.getStatus().toString());
      return new GetIndexInfoResponse(new Response(Response.Status.RPC_ERROR, e.toString()), null);
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
        logInfo("Dropped index for collection `{}` successfully!", collectionName);
        return new Response(Response.Status.SUCCESS);
      } else {
        logError("Drop index for collection `{}` failed:\n{}", collectionName, response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logError("dropIndex RPC failed:\n{}", e.getStatus().toString());
      return new Response(Response.Status.RPC_ERROR, e.toString());
    }
  }

  @Override
  public Response getCollectionStats(String collectionName) {
    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();
    io.milvus.grpc.CollectionInfo response;

    try {
      response = blockingStub.showCollectionInfo(request);

      if (response.getStatus().getErrorCode() == ErrorCode.SUCCESS) {
        logInfo("getCollectionStats for `{}` returned successfully!", collectionName);
        return new Response(Response.Status.SUCCESS, response.getJsonInfo());
      } else {
        logError(
            "getCollectionStats for `{}` failed:\n{}",
            collectionName,
            response.getStatus().toString());
        return new Response(
            Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
            response.getStatus().getReason());
      }
    } catch (StatusRuntimeException e) {
      logError("getCollectionStats RPC failed:\n{}", e.getStatus().toString());
      return new Response(Response.Status.RPC_ERROR, e.toString());
    }
  }

  @Override
  public GetEntityByIDResponse getEntityByID(String collectionName, List<Long> ids) {
    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new GetEntityByIDResponse(
          new Response(Response.Status.CLIENT_NOT_CONNECTED), new ArrayList<>(), null);
    }

    VectorsIdentity request =
        VectorsIdentity.newBuilder().setCollectionName(collectionName).addAllIdArray(ids).build();
    VectorsData response;

    try {
      response = blockingStub.getVectorsByID(request);

      if (response.getStatus().getErrorCode() == ErrorCode.SUCCESS) {

        logInfo("getEntityByID in collection `{}` returned successfully!", collectionName);

        List<List<Float>> floatVectors = new ArrayList<>();
        List<ByteBuffer> binaryVectors = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) {
          floatVectors.add(response.getVectorsData(i).getFloatDataList());
          binaryVectors.add(response.getVectorsData(i).getBinaryData().asReadOnlyByteBuffer());
        }
        return new GetEntityByIDResponse(
            new Response(Response.Status.SUCCESS), floatVectors, binaryVectors);

      } else {
        logError(
            "getEntityByID in collection `{}` failed:\n{}",
            collectionName,
            response.getStatus().toString());
        return new GetEntityByIDResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()),
            new ArrayList<>(),
            null);
      }
    } catch (StatusRuntimeException e) {
      logError("getEntityByID RPC failed:\n{}", e.getStatus().toString());
      return new GetEntityByIDResponse(
          new Response(Response.Status.RPC_ERROR, e.toString()), new ArrayList<>(), null);
    }
  }

  @Override
  public ListIDInSegmentResponse listIDInSegment(String collectionName, String segmentName) {
    if (!channelIsReadyOrIdle()) {
      logWarning("You are not connected to Milvus server");
      return new ListIDInSegmentResponse(
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
            "listIDInSegment in collection `{}`, segment `{}` returned successfully!",
            collectionName,
            segmentName);
        return new ListIDInSegmentResponse(
            new Response(Response.Status.SUCCESS), response.getVectorIdArrayList());
      } else {
        logError(
            "listIDInSegment in collection `{}`, segment `{}` failed:\n{}",
            collectionName,
            segmentName,
            response.getStatus().toString());
        return new ListIDInSegmentResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()),
            new ArrayList<>());
      }
    } catch (StatusRuntimeException e) {
      logError("listIDInSegment RPC failed:\n{}", e.getStatus().toString());
      return new ListIDInSegmentResponse(
          new Response(Response.Status.RPC_ERROR, e.toString()), new ArrayList<>());
    }
  }

  @Override
  public Response deleteEntityByID(String collectionName, List<Long> ids) {
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
        logInfo("deleteEntityByID in collection `{}` completed successfully!", collectionName);
        return new Response(Response.Status.SUCCESS);
      } else {
        logError(
            "deleteEntityByID in collection `{}` failed:\n{}", collectionName, response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logError("deleteEntityByID RPC failed:\n{}", e.getStatus().toString());
      return new Response(Response.Status.RPC_ERROR, e.toString());
    }
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
        logInfo("Flushed collection {} successfully!", collectionNames);
        return new Response(Response.Status.SUCCESS);
      } else {
        logError("Flush collection {} failed:\n{}", collectionNames, response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logError("flush RPC failed:\n{}", e.getStatus().toString());
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
              logInfo("Flushed collection {} successfully!", collectionNames);
            } else {
              logError("Flush collection {} failed:\n{}", collectionNames, result.toString());
            }
          }

          @Override
          public void onFailure(Throwable t) {
            logError("FlushAsync failed:\n{}", t.getMessage());
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
        logInfo("Compacted collection `{}` successfully!", collectionName);
        return new Response(Response.Status.SUCCESS);
      } else {
        logError("Compact collection `{}` failed:\n{}", collectionName, response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logError("compact RPC failed:\n{}", e.getStatus().toString());
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
              logInfo("Compacted collection `{}` successfully!", collectionName);
            } else {
              logError("Compact collection `{}` failed:\n{}", collectionName, result.toString());
            }
          }

          @Override
          public void onFailure(Throwable t) {
            logError("CompactAsync failed:\n{}", t.getMessage());
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
      for (int i = 0; i < numQueries; i++) {
        // Process result of query i
        int pos = i * topK;
        while (pos < i * topK + topK && topKQueryResult.getIdsList().get(pos) != -1) {
          pos++;
        }
        resultIdsList.add(topKQueryResult.getIdsList().subList(i * topK, pos));
        resultDistancesList.add(topKQueryResult.getDistancesList().subList(i * topK, pos));
      }
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
    logger.info(msg, params);
  }

  private void logWarning(String msg, Object... params) {
    logger.warn(msg, params);
  }

  private void logError(String msg, Object... params) {
    logger.error(msg, params);
  }
}
