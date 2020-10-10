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
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.StatusRuntimeException;
import io.milvus.client.exception.ClientSideMilvusException;
import io.milvus.client.exception.InitializationException;
import io.milvus.client.exception.MilvusException;
import io.milvus.client.exception.ServerSideMilvusException;
import io.milvus.client.exception.UnsupportedServerVersion;
import io.milvus.grpc.*;
import org.apache.commons.lang3.ArrayUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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
      Response response = getServerVersion();
      if (response.ok()) {
        String serverVersion = response.getMessage();
        if (!serverVersion.matches("^" + SUPPORTED_SERVER_VERSION + "(\\..*)?$")) {
          throw new UnsupportedServerVersion(connectParam.getTarget(), SUPPORTED_SERVER_VERSION, serverVersion);
        }
      } else {
        throw new InitializationException(connectParam.getTarget(), response.getMessage());
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
  public HasPartitionResponse hasPartition(String collectionName, String tag) {

    if (!maybeAvailable()) {
      logWarning("You are not connected to Milvus server");
      return new HasPartitionResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED), false);
    }

    PartitionParam request =
        PartitionParam.newBuilder().setCollectionName(collectionName).setTag(tag).build();
    BoolReply response;

    try {
      response = blockingStub().hasPartition(request);

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

    if (!maybeAvailable()) {
      logWarning("You are not connected to Milvus server");
      return new ListPartitionsResponse(
          new Response(Response.Status.CLIENT_NOT_CONNECTED), Collections.emptyList());
    }

    CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();
    PartitionList response;

    try {
      response = blockingStub().showPartitions(request);

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
            Collections.emptyList());
      }
    } catch (StatusRuntimeException e) {
      logError("listPartitions RPC failed:\n{}", e.getStatus().toString());
      return new ListPartitionsResponse(
          new Response(Response.Status.RPC_ERROR, e.toString()), Collections.emptyList());
    }
  }

  @Override
  public Response dropPartition(String collectionName, String tag) {

    if (!maybeAvailable()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    PartitionParam request =
        PartitionParam.newBuilder().setCollectionName(collectionName).setTag(tag).build();
    Status response;

    try {
      response = blockingStub().dropPartition(request);

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
  @SuppressWarnings("unchecked")
  public InsertResponse insert(@Nonnull InsertParam insertParam) {

    if (!maybeAvailable()) {
      logWarning("You are not connected to Milvus server");
      return new InsertResponse(
          new Response(Response.Status.CLIENT_NOT_CONNECTED), Collections.emptyList());
    }

    List<FieldValue> fieldValueList = new ArrayList<>();
    List<? extends Map<String, Object>> fields = insertParam.getFields();
    for (Map<String, Object> map : fields) {
      // process each field
      if (!map.containsKey("field") || !map.containsKey("type") ||
          !map.containsKey("values")) {
        logError("insertParam fields map must contain 'field', 'type' and 'values' keys.");
        return new InsertResponse(
            new Response(Response.Status.ILLEGAL_ARGUMENT), Collections.emptyList());
      }
      DataType dataType = (DataType) map.get("type");
      AttrRecord.Builder attrBuilder = AttrRecord.newBuilder();
      VectorRecord.Builder vectorBuilder = VectorRecord.newBuilder();
      try {
        if (dataType == DataType.INT32) {
          attrBuilder.addAllInt32Value((List<Integer>) map.get("values"));
        } else if (dataType == DataType.INT64) {
          attrBuilder.addAllInt64Value((List<Long>) map.get("values"));
        } else if (dataType == DataType.FLOAT) {
          attrBuilder.addAllFloatValue((List<Float>) map.get("values"));
        } else if (dataType == DataType.DOUBLE) {
          attrBuilder.addAllDoubleValue((List<Double>) map.get("values"));
        } else if (dataType == DataType.VECTOR_FLOAT) {
          List<List<Float>> floatVectors = (List<List<Float>>) map.get("values");
          List<VectorRowRecord> vectorRowRecordList = new ArrayList<>();
          for (List<Float> floatVector : floatVectors) {
            vectorRowRecordList.add(
                VectorRowRecord.newBuilder()
                    .addAllFloatData(floatVector)
                    .build());
          }
          vectorBuilder.addAllRecords(vectorRowRecordList);
        } else if (dataType == DataType.VECTOR_BINARY) {
          List<List<Byte>> binaryVectors = (List<List<Byte>>) map.get("values");
          List<VectorRowRecord> vectorRowRecordList = new ArrayList<>();
          for (List<Byte> binaryVector : binaryVectors) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(binaryVector.size());
            for (int i = 0; i < binaryVector.size(); i++) {
              byteBuffer = byteBuffer.put(i, binaryVector.get(i));
            }
            vectorRowRecordList.add(
                VectorRowRecord.newBuilder()
                    .setBinaryData(ByteString.copyFrom(byteBuffer))
                    .build());
          }
          vectorBuilder.addAllRecords(vectorRowRecordList);
        } else {
          logError("insertParam `values` DataType unsupported.");
          return new InsertResponse(
              new Response(Response.Status.ILLEGAL_ARGUMENT), Collections.emptyList());
        }
      } catch (Exception e) {
        logError("insertParam `values` invalid.");
        return new InsertResponse(
            new Response(Response.Status.ILLEGAL_ARGUMENT), Collections.emptyList());
      }

      AttrRecord attrRecord = attrBuilder.build();
      VectorRecord vectorRecord = vectorBuilder.build();

      FieldValue fieldValue =
          FieldValue.newBuilder()
              .setFieldName(map.get("field").toString())
              .setTypeValue(((DataType) map.get("type")).getVal())
              .setAttrRecord(attrRecord)
              .setVectorRecord(vectorRecord)
              .build();
      fieldValueList.add(fieldValue);
    }

    io.milvus.grpc.InsertParam request =
        io.milvus.grpc.InsertParam.newBuilder()
            .setCollectionName(insertParam.getCollectionName())
            .addAllFields(fieldValueList)
            .addAllEntityIdArray(insertParam.getEntityIds())
            .setPartitionTag(insertParam.getPartitionTag())
            .build();

    EntityIds response;

    try {
      response = blockingStub().insert(request);

      if (response.getStatus().getErrorCode() == ErrorCode.SUCCESS) {
        logInfo(
            "Inserted {} vectors to collection `{}` successfully!",
            response.getEntityIdArrayCount(),
            insertParam.getCollectionName());
        return new InsertResponse(
            new Response(Response.Status.SUCCESS), response.getEntityIdArrayList());
      } else {
        logError("Insert vectors failed:\n{}", response.getStatus().toString());
        return new InsertResponse(
            new Response(
                Response.Status.valueOf(response.getStatus().getErrorCodeValue()),
                response.getStatus().getReason()),
            Collections.emptyList());
      }
    } catch (StatusRuntimeException e) {
      logError("insert RPC failed:\n{}", e.getStatus().toString());
      return new InsertResponse(
          new Response(Response.Status.RPC_ERROR, e.toString()), Collections.emptyList());
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public ListenableFuture<InsertResponse> insertAsync(@Nonnull InsertParam insertParam) {

    if (!maybeAvailable()) {
      logWarning("You are not connected to Milvus server");
      return Futures.immediateFuture(
          new InsertResponse(
              new Response(Response.Status.CLIENT_NOT_CONNECTED), Collections.emptyList()));
    }

    List<FieldValue> fieldValueList = new ArrayList<>();
    List<? extends Map<String, Object>> fields = insertParam.getFields();
    for (Map<String, Object> map : fields) {
      // process each field
      if (!map.containsKey("field") || !map.containsKey("type") ||
          !map.containsKey("values")) {
        logError("insertParam fields map must contain 'field', 'type' and 'values' keys.");
        return Futures.immediateFuture(
            new InsertResponse(
                new Response(Response.Status.ILLEGAL_ARGUMENT), Collections.emptyList()));
      }
      DataType dataType = (DataType) map.get("type");
      AttrRecord.Builder attrBuilder = AttrRecord.newBuilder();
      VectorRecord.Builder vectorBuilder = VectorRecord.newBuilder();
      try {
        if (dataType == DataType.INT32) {
          attrBuilder.addAllInt32Value((List<Integer>) map.get("values"));
        } else if (dataType == DataType.INT64) {
          attrBuilder.addAllInt64Value((List<Long>) map.get("values"));
        } else if (dataType == DataType.FLOAT) {
          attrBuilder.addAllFloatValue((List<Float>) map.get("values"));
        } else if (dataType == DataType.DOUBLE) {
          attrBuilder.addAllDoubleValue((List<Double>) map.get("values"));
        } else if (dataType == DataType.VECTOR_FLOAT) {
          List<List<Float>> floatVectors = (List<List<Float>>) map.get("values");
          List<VectorRowRecord> vectorRowRecordList = new ArrayList<>();
          for (List<Float> floatVector : floatVectors) {
            vectorRowRecordList.add(
                VectorRowRecord.newBuilder()
                    .addAllFloatData(floatVector)
                    .build());
          }
          vectorBuilder.addAllRecords(vectorRowRecordList);
        } else if (dataType == DataType.VECTOR_BINARY) {
          List<List<Byte>> binaryVectors = (List<List<Byte>>) map.get("values");
          List<VectorRowRecord> vectorRowRecordList = new ArrayList<>();
          for (List<Byte> binaryVector : binaryVectors) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(binaryVector.size());
            for (int i = 0; i < binaryVector.size(); i++) {
              byteBuffer = byteBuffer.put(i, binaryVector.get(i));
            }
            vectorRowRecordList.add(
                VectorRowRecord.newBuilder()
                    .setBinaryData(ByteString.copyFrom(byteBuffer))
                    .build());
          }
          vectorBuilder.addAllRecords(vectorRowRecordList);
        } else {
          logError("insertParam `values` DataType unsupported.");
          return Futures.immediateFuture(
              new InsertResponse(
                  new Response(Response.Status.ILLEGAL_ARGUMENT), Collections.emptyList()));
        }
      } catch (Exception e) {
        logError("insertParam `values` invalid.");
        return Futures.immediateFuture(
            new InsertResponse(
                new Response(Response.Status.ILLEGAL_ARGUMENT), Collections.emptyList()));
      }

      AttrRecord attrRecord = attrBuilder.build();
      VectorRecord vectorRecord = vectorBuilder.build();

      FieldValue fieldValue =
          FieldValue.newBuilder()
              .setFieldName(map.get("field").toString())
              .setTypeValue(((DataType) map.get("type")).getVal())
              .setAttrRecord(attrRecord)
              .setVectorRecord(vectorRecord)
              .build();
      fieldValueList.add(fieldValue);
    }

    io.milvus.grpc.InsertParam request =
        io.milvus.grpc.InsertParam.newBuilder()
            .setCollectionName(insertParam.getCollectionName())
            .addAllFields(fieldValueList)
            .addAllEntityIdArray(insertParam.getEntityIds())
            .setPartitionTag(insertParam.getPartitionTag())
            .build();

    ListenableFuture<EntityIds> response;

    response = futureStub().insert(request);

    Futures.addCallback(
        response,
        new FutureCallback<EntityIds>() {
          @Override
          public void onSuccess(EntityIds result) {
            if (result.getStatus().getErrorCode() == ErrorCode.SUCCESS) {
              logInfo(
                  "Inserted {} vectors to collection `{}` successfully!",
                  result.getEntityIdArrayCount(),
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

    Function<EntityIds, InsertResponse> transformFunc =
        vectorIds -> {
          if (vectorIds.getStatus().getErrorCode() == ErrorCode.SUCCESS) {
            return new InsertResponse(
                new Response(Response.Status.SUCCESS), vectorIds.getEntityIdArrayList());
          } else {
            return new InsertResponse(
                new Response(
                    Response.Status.valueOf(vectorIds.getStatus().getErrorCodeValue()),
                    vectorIds.getStatus().getReason()),
                Collections.emptyList());
          }
        };

    return Futures.transform(response, transformFunc::apply, MoreExecutors.directExecutor());
  }

  @Override
  public SearchResponse search(@Nonnull SearchParam searchParam) {

    if (!maybeAvailable()) {
      logWarning("You are not connected to Milvus server");
      SearchResponse searchResponse = new SearchResponse();
      searchResponse.setResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED));
      return searchResponse;
    }

    // convert DSL to json object and parse to extract vectors
    List<VectorParam> vectorParamList = new ArrayList<>();
    JSONObject jsonObject;
    List<Object> parsedDSL;
    try {
      jsonObject = new JSONObject(searchParam.getDSL());
      parsedDSL = parseDSL(jsonObject);
    } catch (JSONException err){
      logError("DSL must be in correct json format. Refer to examples for more information.");
      SearchResponse searchResponse = new SearchResponse();
      searchResponse.setResponse(new Response(Response.Status.ILLEGAL_ARGUMENT, err.toString()));
      return searchResponse;
    }
    if (parsedDSL.size() != 3) {
      logError("DSL must include vector query. Refer to examples for more information.");
      SearchResponse searchResponse = new SearchResponse();
      searchResponse.setResponse(new Response(Response.Status.ILLEGAL_ARGUMENT));
      return searchResponse;
    }

    // use placeholder and vectors to create VectorParam list
    String key = parsedDSL.get(2).toString();
    JSONObject outer = (JSONObject) parsedDSL.get(1);
    JSONObject value = (JSONObject) outer.get(key);
    if (!value.has("topk") || !value.has("query") || !value.has("type")) {
      logError("Invalid DSL vector field argument. Refer to examples for more information.");
      SearchResponse searchResponse = new SearchResponse();
      searchResponse.setResponse(new Response(Response.Status.ILLEGAL_ARGUMENT));
      return searchResponse;
    }
    List<VectorRowRecord> vectorRowRecordList = new ArrayList<>();
    if (value.get("type").toString().equals("float")) {
      JSONArray arr = (JSONArray) value.get("query");
      for (int i = 0; i < arr.length(); i++) {
        JSONArray innerArr = (JSONArray) (arr.get(i));
        List<Float> floatList = new ArrayList<>();
        for (int j = 0; j < innerArr.length(); j++) {
          Double num = (Double) innerArr.get(j);
          floatList.add(num.floatValue());
        }
        vectorRowRecordList.add(
            VectorRowRecord.newBuilder()
                .addAllFloatData(floatList)
                .build());
      }
    } else if (value.get("type").toString().equals("binary")) {
      JSONArray arr = (JSONArray) value.get("query");
      for (int i = 0; i < arr.length(); i++) {
        JSONArray innerArr = (JSONArray) (arr.get(i));
        ByteBuffer byteBuffer = ByteBuffer.allocate(innerArr.length());
        for (int j = 0; j < innerArr.length(); j++) {
          byteBuffer = byteBuffer.put(j, ((Integer) innerArr.get(j)).byteValue());
        }
        vectorRowRecordList.add(
            VectorRowRecord.newBuilder()
                .setBinaryData(ByteString.copyFrom(byteBuffer))
                .build());
      }
    } else {
      logError("DSL vector type must be float or binary. Refer to examples for more information.");
      SearchResponse searchResponse = new SearchResponse();
      searchResponse.setResponse(new Response(Response.Status.ILLEGAL_ARGUMENT));
      return searchResponse;
    }

    VectorRecord vectorRecord =
        VectorRecord.newBuilder()
            .addAllRecords(vectorRowRecordList)
            .build();

    JSONObject json = new JSONObject();
    value.remove("type");
    value.remove("query");
    json.put("placeholder", outer);
    VectorParam vectorParam =
        VectorParam.newBuilder()
            .setJson(json.toString())
            .setRowRecord(vectorRecord)
            .build();
    vectorParamList.add(vectorParam);

    KeyValuePair extraParam =
        KeyValuePair.newBuilder()
            .setKey(extraParamKey)
            .setValue(searchParam.getParamsInJson())
            .build();

    io.milvus.grpc.SearchParam request =
        io.milvus.grpc.SearchParam.newBuilder()
            .setCollectionName(searchParam.getCollectionName())
            .setDsl(jsonObject.toString())
            .addAllVectorParam(vectorParamList)
            .addAllPartitionTagArray(searchParam.getPartitionTags())
            .addExtraParams(extraParam)
            .build();

    QueryResult response;

    try {
      response = blockingStub().search(request);

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

    if (!maybeAvailable()) {
      logWarning("You are not connected to Milvus server");
      SearchResponse searchResponse = new SearchResponse();
      searchResponse.setResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED));
      return Futures.immediateFuture(searchResponse);
    }

    // convert DSL to json object and parse to extract vectors
    List<VectorParam> vectorParamList = new ArrayList<>();
    JSONObject jsonObject;
    List<Object> parsedDSL;
    try {
      jsonObject = new JSONObject(searchParam.getDSL());
      parsedDSL = parseDSL(jsonObject);
    } catch (JSONException err){
      logError("DSL must be in correct json format. Refer to examples for more information.");
      SearchResponse searchResponse = new SearchResponse();
      searchResponse.setResponse(new Response(Response.Status.ILLEGAL_ARGUMENT, err.toString()));
      return Futures.immediateFuture(searchResponse);
    }
    if (parsedDSL.size() != 3) {
      logError("DSL must include vector query. Refer to examples for more information.");
      SearchResponse searchResponse = new SearchResponse();
      searchResponse.setResponse(new Response(Response.Status.ILLEGAL_ARGUMENT));
      return Futures.immediateFuture(searchResponse);
    }

    // use placeholder and vectors to create VectorParam list
    String key = parsedDSL.get(2).toString();
    JSONObject outer = (JSONObject) parsedDSL.get(1);
    JSONObject value = (JSONObject) outer.get(key);
    if (!value.has("topk") || !value.has("query") || !value.has("type")) {
      logError("Invalid DSL vector field argument. Refer to examples for more information.");
      SearchResponse searchResponse = new SearchResponse();
      searchResponse.setResponse(new Response(Response.Status.ILLEGAL_ARGUMENT));
      return Futures.immediateFuture(searchResponse);
    }
    List<VectorRowRecord> vectorRowRecordList = new ArrayList<>();
    if (value.get("type").toString().equals("float")) {
      JSONArray arr = (JSONArray) value.get("query");
      for (int i = 0; i < arr.length(); i++) {
        JSONArray innerArr = (JSONArray) (arr.get(i));
        List<Float> floatList = new ArrayList<>();
        for (int j = 0; j < innerArr.length(); j++) {
          Double num = (Double) innerArr.get(j);
          floatList.add(num.floatValue());
        }
        vectorRowRecordList.add(
            VectorRowRecord.newBuilder()
                .addAllFloatData(floatList)
                .build());
      }
    } else if (value.get("type").toString().equals("binary")) {
      JSONArray arr = (JSONArray) value.get("query");
      for (int i = 0; i < arr.length(); i++) {
        JSONArray innerArr = (JSONArray) (arr.get(i));
        ByteBuffer byteBuffer = ByteBuffer.allocate(innerArr.length());
        for (int j = 0; j < innerArr.length(); j++) {
          byteBuffer = byteBuffer.put(j, ((Integer) innerArr.get(j)).byteValue());
        }
        vectorRowRecordList.add(
            VectorRowRecord.newBuilder()
                .setBinaryData(ByteString.copyFrom(byteBuffer))
                .build());
      }
    } else {
      logError("DSL vector type must be float or binary. Refer to examples for more information.");
      SearchResponse searchResponse = new SearchResponse();
      searchResponse.setResponse(new Response(Response.Status.ILLEGAL_ARGUMENT));
      return Futures.immediateFuture(searchResponse);
    }

    VectorRecord vectorRecord =
        VectorRecord.newBuilder()
            .addAllRecords(vectorRowRecordList)
            .build();

    JSONObject json = new JSONObject();
    value.remove("type");
    value.remove("query");
    json.put("placeholder", outer);
    VectorParam vectorParam =
        VectorParam.newBuilder()
            .setJson(json.toString())
            .setRowRecord(vectorRecord)
            .build();
    vectorParamList.add(vectorParam);

    KeyValuePair extraParam =
        KeyValuePair.newBuilder()
            .setKey(extraParamKey)
            .setValue(searchParam.getParamsInJson())
            .build();

    io.milvus.grpc.SearchParam request =
        io.milvus.grpc.SearchParam.newBuilder()
            .setCollectionName(searchParam.getCollectionName())
            .setDsl(jsonObject.toString())
            .addAllVectorParam(vectorParamList)
            .addAllPartitionTagArray(searchParam.getPartitionTags())
            .addExtraParams(extraParam)
            .build();

    ListenableFuture<QueryResult> response;

    response = futureStub().search(request);

    Futures.addCallback(
        response,
        new FutureCallback<QueryResult>() {
          @Override
          public void onSuccess(QueryResult result) {
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

    Function<QueryResult, SearchResponse> transformFunc =
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

    if (!maybeAvailable()) {
      logWarning("You are not connected to Milvus server");
      return new GetCollectionInfoResponse(
          new Response(Response.Status.CLIENT_NOT_CONNECTED), null);
    }

    CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();
    Mapping response;

    try {
      response = blockingStub().describeCollection(request);

      if (response.getStatus().getErrorCode() == ErrorCode.SUCCESS) {
        CollectionMapping collectionMapping = new CollectionMapping(response);
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

    if (!maybeAvailable()) {
      logWarning("You are not connected to Milvus server");
      return new ListCollectionsResponse(
          new Response(Response.Status.CLIENT_NOT_CONNECTED), Collections.emptyList());
    }

    Command request = Command.newBuilder().setCmd("").build();
    CollectionNameList response;

    try {
      response = blockingStub().showCollections(request);

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
            Collections.emptyList());
      }
    } catch (StatusRuntimeException e) {
      logError("listCollections RPC failed:\n{}", e.getStatus().toString());
      return new ListCollectionsResponse(
          new Response(Response.Status.RPC_ERROR, e.toString()), Collections.emptyList());
    }
  }

  @Override
  public CountEntitiesResponse countEntities(@Nonnull String collectionName) {

    if (!maybeAvailable()) {
      logWarning("You are not connected to Milvus server");
      return new CountEntitiesResponse(new Response(Response.Status.CLIENT_NOT_CONNECTED), 0);
    }

    CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();
    CollectionRowCount response;

    try {
      response = blockingStub().countCollection(request);

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

    if (!maybeAvailable()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    Command request = Command.newBuilder().setCmd(command).build();
    StringReply response;

    try {
      response = blockingStub().cmd(request);

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

    if (!maybeAvailable()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    CollectionName request = CollectionName.newBuilder().setCollectionName(collectionName).build();
    Status response;

    try {
      response = blockingStub().preloadCollection(request);

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
  public Response dropIndex(String collectionName, String fieldName) {

    if (!maybeAvailable()) {
      logWarning("You are not connected to Milvus server");
      return new Response(Response.Status.CLIENT_NOT_CONNECTED);
    }

    IndexParam request =
        IndexParam.newBuilder()
            .setCollectionName(collectionName)
            .setFieldName(fieldName)
            .build();
    Status response;

    try {
      response = blockingStub().dropIndex(request);

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

  private SearchResponse buildSearchResponse(QueryResult topKQueryResult) {

    final int numQueries = (int) topKQueryResult.getRowNum();
    final int topK =
        numQueries == 0
            ? 0
            : topKQueryResult.getDistancesCount()
                / numQueries; // Guaranteed to be divisible from server side

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
              ByteBuffer bb = vectorRowRecordList.get(j).getBinaryData().asReadOnlyByteBuffer();
              byte[] b = new byte[bb.remaining()];
              bb.get(b);
              fieldsMap.get(j).put(fieldName, Arrays.asList(ArrayUtils.toObject(b)));
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

    SearchResponse searchResponse = new SearchResponse();
    searchResponse.setNumQueries(numQueries);
    searchResponse.setTopK(topK);
    searchResponse.setResultIdsList(resultIdsList);
    searchResponse.setResultDistancesList(resultDistancesList);
    searchResponse.setFieldsMap(resultFieldsMap);

    return searchResponse;
  }

  private String kvListToString(List<KeyValuePair> kv) {
    JSONObject jsonObject = new JSONObject();
    for (KeyValuePair keyValuePair : kv) {
      if (keyValuePair.getValue().equals("null")) continue;
      jsonObject.put(keyValuePair.getKey(), keyValuePair.getValue());
    }
    return jsonObject.toString();
  }

  private List<Object> parseDSL(JSONObject dsl) {
    Iterator<String> keys = dsl.keys();
    while (keys.hasNext()) {
      String key = keys.next();
      if (key.equals("vector")) {
        // replace dsl vector data by a placeholder string
        List<Object> res = new ArrayList<>();
        JSONObject vecData = (JSONObject) dsl.get(key);
        String name = vecData.keys().next();
        dsl.put(key, "placeholder");
        res.add(dsl);
        res.add(vecData);
        res.add(name);
        return res;
      }
      if (dsl.get(key).getClass() == JSONObject.class) {
        List<Object> res = parseDSL((JSONObject) dsl.get(key));
        if (res.size() > 0) return res;
      } else if (dsl.get(key).getClass() == JSONArray.class) {
        JSONArray arr = (JSONArray) dsl.get(key);
        for (int i = 0; i < arr.length(); i++) {
          JSONObject jsonObject = arr.getJSONObject(i);
          List<Object> res = parseDSL(jsonObject);
          if (res.size() > 0) return res;
        }
      }
    }
    return new ArrayList<>();
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
