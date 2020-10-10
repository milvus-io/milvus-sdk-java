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
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.StatusRuntimeException;
import io.milvus.client.exception.ClientSideMilvusException;
import io.milvus.client.exception.MilvusException;
import io.milvus.client.exception.ServerSideMilvusException;
import io.milvus.client.exception.UnsupportedServerVersion;
import io.milvus.grpc.*;
import org.apache.commons.lang3.ArrayUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/** Actual implementation of interface <code>MilvusClient</code> */
public class MilvusGrpcClient extends AbstractMilvusGrpcClient {

  private static final Logger logger = LoggerFactory.getLogger(MilvusGrpcClient.class);
  private static final String SUPPORTED_SERVER_VERSION = "0.11";

  private final String target;
  private final ManagedChannel channel;
  private final MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub;
  private final MilvusServiceGrpc.MilvusServiceFutureStub futureStub;

  public MilvusGrpcClient(ConnectParam connectParam) {
    target = connectParam.getTarget();
    channel = ManagedChannelBuilder
        .forTarget(connectParam.getTarget())
        .usePlaintext()
        .maxInboundMessageSize(Integer.MAX_VALUE)
        .defaultLoadBalancingPolicy(connectParam.getDefaultLoadBalancingPolicy())
        .keepAliveTime(connectParam.getKeepAliveTime(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS)
        .keepAliveTimeout(connectParam.getKeepAliveTimeout(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS)
        .keepAliveWithoutCalls(connectParam.isKeepAliveWithoutCalls())
        .idleTimeout(connectParam.getIdleTimeout(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS)
        .build();
    blockingStub = MilvusServiceGrpc.newBlockingStub(channel);
    futureStub = MilvusServiceGrpc.newFutureStub(channel);
    try {
      String serverVersion = getServerVersion();
      if (!serverVersion.matches("^" + SUPPORTED_SERVER_VERSION + "(\\..*)?$")) {
        throw new UnsupportedServerVersion(connectParam.getTarget(), SUPPORTED_SERVER_VERSION, serverVersion);
      }
    } catch (Throwable t) {
      channel.shutdownNow();
      throw t;
    }
  }

  @Override
  public String target() {
    return target;
  }

  @Override
  protected MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub() {
    return blockingStub;
  }

  @Override
  protected MilvusServiceGrpc.MilvusServiceFutureStub futureStub() {
    return futureStub;
  }

  @Override
  protected boolean maybeAvailable() {
    switch (channel.getState(false)) {
      case IDLE:
      case CONNECTING:
      case READY:
        return true;
      default:
        return false;
    }
  }

  @Override
  public void close(long maxWaitSeconds) {
    channel.shutdown();
    try {
      channel.awaitTermination(maxWaitSeconds, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      logger.warn("Milvus client close interrupted");
      channel.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  public MilvusClient withTimeout(long timeout, TimeUnit timeoutUnit) {
    final long timeoutMillis = timeoutUnit.toMillis(timeout);
    final TimeoutInterceptor timeoutInterceptor = new TimeoutInterceptor(timeoutMillis);
    final MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub =
        this.blockingStub.withInterceptors(timeoutInterceptor);
    final MilvusServiceGrpc.MilvusServiceFutureStub futureStub =
        this.futureStub.withInterceptors(timeoutInterceptor);

    return new AbstractMilvusGrpcClient() {
      @Override
      public String target() {
        return MilvusGrpcClient.this.target();
      }

      @Override
      protected MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub() {
        return blockingStub;
      }

      @Override
      protected MilvusServiceGrpc.MilvusServiceFutureStub futureStub() {
        return futureStub;
      }

      @Override
      protected boolean maybeAvailable() {
        return MilvusGrpcClient.this.maybeAvailable();
      }

      @Override
      public void close(long maxWaitSeconds) {
        MilvusGrpcClient.this.close(maxWaitSeconds);
      }

      @Override
      public MilvusClient withTimeout(long timeout, TimeUnit timeoutUnit) {
        return MilvusGrpcClient.this.withTimeout(timeout, timeoutUnit);
      }
    };
  }

  private static class TimeoutInterceptor implements ClientInterceptor {
    private long timeoutMillis;

    TimeoutInterceptor(long timeoutMillis) {
      this.timeoutMillis = timeoutMillis;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
      return next.newCall(method, callOptions.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS));
    }
  }
}

abstract class AbstractMilvusGrpcClient implements MilvusClient {
  private static final Logger logger = LoggerFactory.getLogger(AbstractMilvusGrpcClient.class);

  protected abstract MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub();

  protected abstract MilvusServiceGrpc.MilvusServiceFutureStub futureStub();

  protected abstract boolean maybeAvailable();

  private void translateExceptions(Runnable body) {
    translateExceptions(() -> {
      body.run();
      return null;
    });
  }

  @SuppressWarnings("unchecked")
  private <T> T translateExceptions(Supplier<T> body) {
    try {
      T result = body.get();
      if (result instanceof ListenableFuture) {
        ListenableFuture futureResult = (ListenableFuture) result;
        result = (T) Futures.catching(
            futureResult, Throwable.class, this::translate, MoreExecutors.directExecutor());
      }
      return result;
    } catch (Throwable e) {
      return translate(e);
    }
  }

  private <R> R translate(Throwable e) {
    if (e instanceof MilvusException) {
      throw (MilvusException) e;
    } else if (e.getCause() == null || e.getCause() == e) {
      throw new ClientSideMilvusException(target(), e);
    } else {
      return translate(e.getCause());
    }
  }

  private Void checkResponseStatus(Status status) {
    if (status.getErrorCode() != ErrorCode.SUCCESS) {
      throw new ServerSideMilvusException(target(), status);
    }
    return null;
  }

  @Override
  public void createCollection(@Nonnull CollectionMapping collectionMapping) {
    translateExceptions(() -> {
      Status response = blockingStub().createCollection(collectionMapping.grpc());
      checkResponseStatus(response);
    });
  }

  @Override
  public boolean hasCollection(@Nonnull String collectionName) {
    return translateExceptions(() -> {
      CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();
      BoolReply response = blockingStub().hasCollection(request);
      checkResponseStatus(response.getStatus());
      return response.getBoolReply();
    });
  }

  @Override
  public void dropCollection(@Nonnull String collectionName) {
    translateExceptions(() -> {
      CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();
      Status response = blockingStub().dropCollection(request);
      checkResponseStatus(response);
    });
  }

  @Override
  public void createIndex(@Nonnull Index index) {
    translateExceptions(() -> {
      Futures.getUnchecked(createIndexAsync(index));
    });
  }

  @Override
  public ListenableFuture<Void> createIndexAsync(@Nonnull Index index) {
    return translateExceptions(() -> {
      IndexParam request = index.grpc();
      ListenableFuture<Status> responseFuture = futureStub().createIndex(request);
      return Futures.transform(responseFuture, this::checkResponseStatus, MoreExecutors.directExecutor());
    });
  }

  @Override
  public void createPartition(String collectionName, String tag) {
    translateExceptions(() -> {
      PartitionParam request = PartitionParam.newBuilder().setCollectionName(collectionName).setTag(tag).build();
      Status response = blockingStub().createPartition(request);
      checkResponseStatus(response);
    });
  }

  @Override
  public boolean hasPartition(String collectionName, String tag) {
    return translateExceptions(() -> {
      PartitionParam request = PartitionParam.newBuilder().setCollectionName(collectionName).setTag(tag).build();
      BoolReply response = blockingStub().hasPartition(request);
      checkResponseStatus(response.getStatus());
      return response.getBoolReply();
    });
  }

  @Override
  public List<String> listPartitions(String collectionName) {
    return translateExceptions(() -> {
      CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();
      PartitionList response = blockingStub().showPartitions(request);
      checkResponseStatus(response.getStatus());
      return response.getPartitionTagArrayList();
    });
  }

  @Override
  public void dropPartition(String collectionName, String tag) {
    translateExceptions(() -> {
      PartitionParam request =
          PartitionParam.newBuilder().setCollectionName(collectionName).setTag(tag).build();
      Status response = blockingStub().dropPartition(request);
      checkResponseStatus(response);
    });
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Long> insert(@Nonnull InsertParam insertParam) {
    return translateExceptions(() -> Futures.getUnchecked(insertAsync(insertParam)));
  }

  @Override
  @SuppressWarnings("unchecked")
  public ListenableFuture<List<Long>> insertAsync(@Nonnull InsertParam insertParam) {
    return translateExceptions(() -> {
      io.milvus.grpc.InsertParam request = insertParam.grpc();
      ListenableFuture<EntityIds> responseFuture = futureStub().insert(request);
      return Futures.transform(responseFuture, entityIds -> {
        checkResponseStatus(entityIds.getStatus());
        return entityIds.getEntityIdArrayList();
      }, MoreExecutors.directExecutor());
    });
  }

  @Override
  public SearchResult search(@Nonnull SearchParam searchParam) {
    return translateExceptions(() -> Futures.getUnchecked(searchAsync(searchParam)));
  }

  @Override
  public ListenableFuture<SearchResult> searchAsync(@Nonnull SearchParam searchParam) {
    return translateExceptions(() -> {
      io.milvus.grpc.SearchParam request = searchParam.grpc();
      ListenableFuture<QueryResult> responseFuture = futureStub().search(request);
      return Futures.transform(responseFuture, queryResult -> {
        checkResponseStatus(queryResult.getStatus());
        return buildSearchResponse(queryResult);
      }, MoreExecutors.directExecutor());
    });
  }

  @Override
  public CollectionMapping getCollectionInfo(@Nonnull String collectionName) {
    return translateExceptions(() -> {
      CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();
      Mapping response = blockingStub().describeCollection(request);
      checkResponseStatus(response.getStatus());
      return new CollectionMapping(response);
    });
  }

  @Override
  public List<String> listCollections() {
    return translateExceptions(() -> {
      Command request = Command.newBuilder().setCmd("").build();
      CollectionNameList response = blockingStub().showCollections(request);
      checkResponseStatus(response.getStatus());
      return response.getCollectionNamesList();
    });
  }

  @Override
  public long countEntities(@Nonnull String collectionName) {
    return translateExceptions(() -> {
      CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();
      CollectionRowCount response = blockingStub().countCollection(request);
      checkResponseStatus(response.getStatus());
      return response.getCollectionRowCount();
    });
  }

  @Override
  public String getServerStatus() {
    return command("status");
  }

  @Override
  public String getServerVersion() {
    return command("version");
  }

  public String command(@Nonnull String command) {
    return translateExceptions(() -> {
      Command request = Command.newBuilder().setCmd(command).build();
      StringReply response = blockingStub().cmd(request);
      checkResponseStatus(response.getStatus());
      return response.getStringReply();
    });
  }

  @Override
  public void loadCollection(@Nonnull String collectionName) {
    translateExceptions(() -> {
      CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();
      Status response = blockingStub().preloadCollection(request);
      checkResponseStatus(response);
    });
  }

  @Override
  public void dropIndex(String collectionName, String fieldName) {
    translateExceptions(() -> {
      IndexParam request = IndexParam.newBuilder()
          .setCollectionName(collectionName)
          .setFieldName(fieldName)
          .build();
      Status response = blockingStub().dropIndex(request);
      checkResponseStatus(response);
    });
  }

  @Override
  public Response getCollectionStats(String collectionName) {
    if (!maybeAvailable()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();
    io.milvus.grpc.CollectionInfo response;

    try {
      response = blockingStub().showCollectionInfo(request);

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
  public GetEntityByIDResponse getEntityByID(String collectionName, List<Long> ids, List<String> fieldNames) {
    if (!maybeAvailable()) {
      logWarning("You are not connected to Milvus server");
      return new GetEntityByIDResponse(
          new Response(Response.Status.CLIENT_NOT_CONNECTED), Collections.emptyList());
    }

    EntityIdentity request =
        EntityIdentity.newBuilder()
            .setCollectionName(collectionName)
            .addAllIdArray(ids)
            .addAllFieldNames(fieldNames)
            .build();
    Entities response;

    try {
      response = blockingStub().getEntityByID(request);

      if (response.getStatus().getErrorCode() == ErrorCode.SUCCESS) {

        logInfo("getEntityByID in collection `{}` returned successfully!", collectionName);

        List<Map<String, Object>> fieldsMap = new ArrayList<>();
        List<Boolean> isValid = response.getValidRowList();
        for (int i = 0; i < isValid.size(); i++) {
          fieldsMap.add(new HashMap<>());
        }
        List<FieldValue> fieldValueList = response.getFieldsList();
        for (FieldValue fieldValue : fieldValueList) {
          String fieldName = fieldValue.getFieldName();
          for (int j = 0; j < isValid.size(); j++) {
            if (!isValid.get(j)) continue;
            if (fieldValue.getAttrRecord().getInt32ValueCount() > 0) {
              fieldsMap.get(j)
                  .put(fieldName, fieldValue.getAttrRecord().getInt32ValueList().get(j));
            } else if (fieldValue.getAttrRecord().getInt64ValueCount() > 0) {
              fieldsMap.get(j)
                  .put(fieldName, fieldValue.getAttrRecord().getInt64ValueList().get(j));
            } else if (fieldValue.getAttrRecord().getDoubleValueCount() > 0) {
              fieldsMap.get(j)
                  .put(fieldName, fieldValue.getAttrRecord().getDoubleValueList().get(j));
            } else if (fieldValue.getAttrRecord().getFloatValueCount() > 0) {
              fieldsMap.get(j)
                  .put(fieldName, fieldValue.getAttrRecord().getFloatValueList().get(j));
            } else {
              // the object is vector
              List<VectorRowRecord> vectorRowRecordList =
                  fieldValue.getVectorRecord().getRecordsList();
              if (vectorRowRecordList.get(j).getFloatDataCount() > 0) {
                fieldsMap.get(j).put(fieldName, vectorRowRecordList.get(j).getFloatDataList());
              } else {
                ByteBuffer bb = vectorRowRecordList.get(j).getBinaryData().asReadOnlyByteBuffer();
                byte[] b = new byte[bb.remaining()];
                bb.get(b);
                fieldsMap.get(j).put(fieldName, Arrays.asList(ArrayUtils.toObject(b)));
              }
            }
          }
        }
        return new GetEntityByIDResponse(
            new Response(Response.Status.SUCCESS), fieldsMap);

      } else {
        logError(
            "getEntityByID in collection `{}` failed:\n{}",
            collectionName,
            response.getStatus().toString());
        return new GetEntityByIDResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()),
            Collections.emptyList());
      }
    } catch (StatusRuntimeException e) {
      logError("getEntityByID RPC failed:\n{}", e.getStatus().toString());
      return new GetEntityByIDResponse(
          new Response(Response.Status.RPC_ERROR, e.toString()), Collections.emptyList());
    }
  }

  @Override
  public GetEntityByIDResponse getEntityByID(String collectionName, List<Long> ids) {
    return getEntityByID(collectionName, ids, Collections.emptyList());
  }

  @Override
  public ListIDInSegmentResponse listIDInSegment(String collectionName, Long segmentId) {
    if (!maybeAvailable()) {
      logWarning("You are not connected to Milvus server");
      return new ListIDInSegmentResponse(
          new Response(Response.Status.CLIENT_NOT_CONNECTED), Collections.emptyList());
    }

    GetEntityIDsParam request =
        GetEntityIDsParam.newBuilder()
            .setCollectionName(collectionName)
            .setSegmentId(segmentId)
            .build();
    EntityIds response;

    try {
      response = blockingStub().getEntityIDs(request);

      if (response.getStatus().getErrorCode() == ErrorCode.SUCCESS) {

        logInfo(
            "listIDInSegment in collection `{}`, segment `{}` returned successfully!",
            collectionName,
            segmentId);
        return new ListIDInSegmentResponse(
            new Response(Response.Status.SUCCESS), response.getEntityIdArrayList());
      } else {
        logError(
            "listIDInSegment in collection `{}`, segment `{}` failed:\n{}",
            collectionName,
            segmentId,
            response.getStatus().toString());
        return new ListIDInSegmentResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()),
            Collections.emptyList());
      }
    } catch (StatusRuntimeException e) {
      logError("listIDInSegment RPC failed:\n{}", e.getStatus().toString());
      return new ListIDInSegmentResponse(
          new Response(Response.Status.RPC_ERROR, e.toString()), Collections.emptyList());
    }
  }

  @Override
  public Response deleteEntityByID(String collectionName, List<Long> ids) {
    if (!maybeAvailable()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    DeleteByIDParam request =
        DeleteByIDParam.newBuilder().setCollectionName(collectionName).addAllIdArray(ids).build();
    Status response;

    try {
      response = blockingStub().deleteByID(request);

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
    if (!maybeAvailable()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    FlushParam request = FlushParam.newBuilder().addAllCollectionNameArray(collectionNames).build();
    Status response;

    try {
      response = blockingStub().flush(request);

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

    if (!maybeAvailable()) {
      logWarning("You are not connected to Milvus server");
      return Futures.immediateFuture(new Response(Response.Status.CLIENT_NOT_CONNECTED));
    }

    FlushParam request = FlushParam.newBuilder().addAllCollectionNameArray(collectionNames).build();

    ListenableFuture<Status> response;

    response = futureStub().flush(request);

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
  public Response compact(CompactParam compactParam) {
    if (!maybeAvailable()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    io.milvus.grpc.CompactParam request =
        io.milvus.grpc.CompactParam.newBuilder()
            .setCollectionName(compactParam.getCollectionName())
            .setThreshold(compactParam.getThreshold())
            .build();
    Status response;

    try {
      response = blockingStub().compact(request);

      if (response.getErrorCode() == ErrorCode.SUCCESS) {
        logInfo("Compacted collection `{}` successfully!", compactParam.getCollectionName());
        return new Response(Response.Status.SUCCESS);
      } else {
        logError("Compact collection `{}` failed:\n{}",
            compactParam.getCollectionName(), response.toString());
        return new Response(
            Response.Status.valueOf(response.getErrorCodeValue()), response.getReason());
      }
    } catch (StatusRuntimeException e) {
      logError("compact RPC failed:\n{}", e.getStatus().toString());
      return new Response(Response.Status.RPC_ERROR, e.toString());
    }
  }

  @Override
  public ListenableFuture<Response> compactAsync(@Nonnull CompactParam compactParam) {

    if (!maybeAvailable()) {
      logWarning("You are not connected to Milvus server");
      return Futures.immediateFuture(new Response(Response.Status.CLIENT_NOT_CONNECTED));
    }

    io.milvus.grpc.CompactParam request =
        io.milvus.grpc.CompactParam.newBuilder()
            .setCollectionName(compactParam.getCollectionName())
            .setThreshold(compactParam.getThreshold())
            .build();

    ListenableFuture<Status> response;

    response = futureStub().compact(request);

    Futures.addCallback(
        response,
        new FutureCallback<Status>() {
          @Override
          public void onSuccess(Status result) {
            if (result.getErrorCode() == ErrorCode.SUCCESS) {
              logInfo("Compacted collection `{}` successfully!",
                  compactParam.getCollectionName());
            } else {
              logError("Compact collection `{}` failed:\n{}",
                  compactParam.getCollectionName(), result.toString());
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

  private SearchResult buildSearchResponse(QueryResult topKQueryResult) {
    final int numQueries = (int) topKQueryResult.getRowNum();
    final int topK = numQueries == 0 ? 0 : topKQueryResult.getDistancesCount() / numQueries;

    List<List<Long>> resultIdsList = new ArrayList<>(numQueries);
    List<List<Float>> resultDistancesList = new ArrayList<>(numQueries);
    List<List<Map<String, Object>>> resultFieldsMap = new ArrayList<>(numQueries);

    Entities entities = topKQueryResult.getEntities();
    List<Long> queryIdsList = entities.getIdsList();
    List<Float> queryDistancesList = topKQueryResult.getDistancesList();

    // If fields specified, put it into searchResponse
    List<Map<String, Object>> fieldsMap = new ArrayList<>();
    for (int i = 0; i < queryIdsList.size(); i++) {
      fieldsMap.add(new HashMap<>());
    }
    if (entities.getValidRowCount() != 0) {
      List<FieldValue> fieldValueList = entities.getFieldsList();
      for (FieldValue fieldValue : fieldValueList) {
        String fieldName = fieldValue.getFieldName();
        for (int j = 0; j < queryIdsList.size(); j++) {
          if (fieldValue.getAttrRecord().getInt32ValueCount() > 0) {
            fieldsMap.get(j).put(fieldName, fieldValue.getAttrRecord().getInt32ValueList().get(j));
          } else if (fieldValue.getAttrRecord().getInt64ValueCount() > 0) {
            fieldsMap.get(j).put(fieldName, fieldValue.getAttrRecord().getInt64ValueList().get(j));
          } else if (fieldValue.getAttrRecord().getDoubleValueCount() > 0) {
            fieldsMap.get(j).put(fieldName, fieldValue.getAttrRecord().getDoubleValueList().get(j));
          } else if (fieldValue.getAttrRecord().getFloatValueCount() > 0) {
            fieldsMap.get(j).put(fieldName, fieldValue.getAttrRecord().getFloatValueList().get(j));
          } else {
            // the object is vector
            List<VectorRowRecord> vectorRowRecordList =
                fieldValue.getVectorRecord().getRecordsList();
            if (vectorRowRecordList.get(j).getFloatDataCount() > 0) {
              fieldsMap.get(j).put(fieldName, vectorRowRecordList.get(j).getFloatDataList());
            } else {
              fieldsMap.get(j).put(fieldName, vectorRowRecordList.get(j).getBinaryData().asReadOnlyByteBuffer());
            }
          }
        }
      }
    }

    if (topK > 0) {
      for (int i = 0; i < numQueries; i++) {
        // Process result of query i
        int pos = i * topK;
        while (pos < i * topK + topK && queryIdsList.get(pos) != -1) {
          pos++;
        }
        resultIdsList.add(queryIdsList.subList(i * topK, pos));
        resultDistancesList.add(queryDistancesList.subList(i * topK, pos));
        resultFieldsMap.add(fieldsMap.subList(i * topK, pos));
      }
    }

    return new SearchResult(numQueries, topK, resultIdsList, resultDistancesList, resultFieldsMap);
  }

  private String kvListToString(List<KeyValuePair> kv) {
    JSONObject jsonObject = new JSONObject();
    for (KeyValuePair keyValuePair : kv) {
      if (keyValuePair.getValue().equals("null")) continue;
      jsonObject.put(keyValuePair.getKey(), keyValuePair.getValue());
    }
    return jsonObject.toString();
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
