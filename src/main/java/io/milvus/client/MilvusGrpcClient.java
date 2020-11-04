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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.milvus.client.exception.ClientSideMilvusException;
import io.milvus.client.exception.MilvusException;
import io.milvus.client.exception.ServerSideMilvusException;
import io.milvus.client.exception.UnsupportedServerVersion;
import io.milvus.grpc.AttrRecord;
import io.milvus.grpc.BoolReply;
import io.milvus.grpc.CollectionInfo;
import io.milvus.grpc.CollectionName;
import io.milvus.grpc.CollectionNameList;
import io.milvus.grpc.CollectionRowCount;
import io.milvus.grpc.Command;
import io.milvus.grpc.DeleteByIDParam;
import io.milvus.grpc.Entities;
import io.milvus.grpc.EntityIdentity;
import io.milvus.grpc.EntityIds;
import io.milvus.grpc.ErrorCode;
import io.milvus.grpc.FieldValue;
import io.milvus.grpc.FlushParam;
import io.milvus.grpc.GetEntityIDsParam;
import io.milvus.grpc.IndexParam;
import io.milvus.grpc.Mapping;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.PartitionList;
import io.milvus.grpc.PartitionParam;
import io.milvus.grpc.QueryResult;
import io.milvus.grpc.Status;
import io.milvus.grpc.StringReply;
import io.milvus.grpc.VectorRecord;
import io.milvus.grpc.VectorRowRecord;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    channel =
        ManagedChannelBuilder.forTarget(connectParam.getTarget())
            .usePlaintext()
            .maxInboundMessageSize(Integer.MAX_VALUE)
            .defaultLoadBalancingPolicy(connectParam.getDefaultLoadBalancingPolicy())
            .keepAliveTime(
                connectParam.getKeepAliveTime(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS)
            .keepAliveTimeout(
                connectParam.getKeepAliveTimeout(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS)
            .keepAliveWithoutCalls(connectParam.isKeepAliveWithoutCalls())
            .idleTimeout(connectParam.getIdleTimeout(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS)
            .build();
    blockingStub = MilvusServiceGrpc.newBlockingStub(channel);
    futureStub = MilvusServiceGrpc.newFutureStub(channel);
    try {
      String serverVersion = getServerVersion();
      if (!serverVersion.matches("^" + SUPPORTED_SERVER_VERSION + "(\\..*)?$")) {
        throw new UnsupportedServerVersion(
            connectParam.getTarget(), SUPPORTED_SERVER_VERSION, serverVersion);
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

  public MilvusClient withLogging() {
    return withLogging(LoggingAdapter.DEFAULT_LOGGING_ADAPTER);
  }

  public MilvusClient withLogging(LoggingAdapter loggingAdapter) {
    return withInterceptors(new LoggingInterceptor(loggingAdapter));
  }

  public MilvusClient withTimeout(long timeout, TimeUnit timeoutUnit) {
    final long timeoutMillis = timeoutUnit.toMillis(timeout);
    final TimeoutInterceptor timeoutInterceptor = new TimeoutInterceptor(timeoutMillis);
    return withInterceptors(timeoutInterceptor);
  }

  private MilvusClient withInterceptors(ClientInterceptor... interceptors) {
    final MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub =
        this.blockingStub.withInterceptors(interceptors);
    final MilvusServiceGrpc.MilvusServiceFutureStub futureStub =
        this.futureStub.withInterceptors(interceptors);

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
    private final long timeoutMillis;

    TimeoutInterceptor(long timeoutMillis) {
      this.timeoutMillis = timeoutMillis;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
      return next.newCall(
          method, callOptions.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS));
    }
  }

  private static class LoggingInterceptor implements ClientInterceptor {
    private final LoggingAdapter loggingAdapter;

    LoggingInterceptor(LoggingAdapter loggingAdapter) {
      this.loggingAdapter = loggingAdapter;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
      return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
          next.newCall(method, callOptions)) {
        private final String traceId = loggingAdapter.getTraceId();

        @Override
        public void sendMessage(ReqT message) {
          loggingAdapter.logRequest(logger, traceId, method, message);
          super.sendMessage(message);
        }

        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
          super.start(
              new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(
                  responseListener) {
                @Override
                public void onMessage(RespT message) {
                  loggingAdapter.logResponse(logger, traceId, method, message);
                  super.onMessage(message);
                }
              },
              headers);
        }
      };
    }
  }
}

abstract class AbstractMilvusGrpcClient implements MilvusClient {
  protected abstract MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub();

  protected abstract MilvusServiceGrpc.MilvusServiceFutureStub futureStub();

  private void translateExceptions(Runnable body) {
    translateExceptions(
        () -> {
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
        result =
            (T)
                Futures.catching(
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
    translateExceptions(
        () -> {
          Status response = blockingStub().createCollection(collectionMapping.grpc());
          checkResponseStatus(response);
        });
  }

  @Override
  public boolean hasCollection(@Nonnull String collectionName) {
    return translateExceptions(
        () -> {
          CollectionName request =
              CollectionName.newBuilder().setCollectionName(collectionName).build();
          BoolReply response = blockingStub().hasCollection(request);
          checkResponseStatus(response.getStatus());
          return response.getBoolReply();
        });
  }

  @Override
  public void dropCollection(@Nonnull String collectionName) {
    translateExceptions(
        () -> {
          CollectionName request =
              CollectionName.newBuilder().setCollectionName(collectionName).build();
          Status response = blockingStub().dropCollection(request);
          checkResponseStatus(response);
        });
  }

  @Override
  public void createIndex(@Nonnull Index index) {
    translateExceptions(
        () -> {
          Futures.getUnchecked(createIndexAsync(index));
        });
  }

  @Override
  public ListenableFuture<Void> createIndexAsync(@Nonnull Index index) {
    return translateExceptions(
        () -> {
          IndexParam request = index.grpc();
          ListenableFuture<Status> responseFuture = futureStub().createIndex(request);
          return Futures.transform(
              responseFuture, this::checkResponseStatus, MoreExecutors.directExecutor());
        });
  }

  @Override
  public void createPartition(String collectionName, String tag) {
    translateExceptions(
        () -> {
          PartitionParam request =
              PartitionParam.newBuilder().setCollectionName(collectionName).setTag(tag).build();
          Status response = blockingStub().createPartition(request);
          checkResponseStatus(response);
        });
  }

  @Override
  public boolean hasPartition(String collectionName, String tag) {
    return translateExceptions(
        () -> {
          PartitionParam request =
              PartitionParam.newBuilder().setCollectionName(collectionName).setTag(tag).build();
          BoolReply response = blockingStub().hasPartition(request);
          checkResponseStatus(response.getStatus());
          return response.getBoolReply();
        });
  }

  @Override
  public List<String> listPartitions(String collectionName) {
    return translateExceptions(
        () -> {
          CollectionName request =
              CollectionName.newBuilder().setCollectionName(collectionName).build();
          PartitionList response = blockingStub().showPartitions(request);
          checkResponseStatus(response.getStatus());
          return response.getPartitionTagArrayList();
        });
  }

  @Override
  public void dropPartition(String collectionName, String tag) {
    translateExceptions(
        () -> {
          PartitionParam request =
              PartitionParam.newBuilder().setCollectionName(collectionName).setTag(tag).build();
          Status response = blockingStub().dropPartition(request);
          checkResponseStatus(response);
        });
  }

  @Override
  public List<Long> insert(@Nonnull InsertParam insertParam) {
    return translateExceptions(() -> Futures.getUnchecked(insertAsync(insertParam)));
  }

  @Override
  public ListenableFuture<List<Long>> insertAsync(@Nonnull InsertParam insertParam) {
    return translateExceptions(
        () -> {
          io.milvus.grpc.InsertParam request = insertParam.grpc();
          ListenableFuture<EntityIds> responseFuture = futureStub().insert(request);
          return Futures.transform(
              responseFuture,
              entityIds -> {
                checkResponseStatus(entityIds.getStatus());
                return entityIds.getEntityIdArrayList();
              },
              MoreExecutors.directExecutor());
        });
  }

  @Override
  public SearchResult search(@Nonnull SearchParam searchParam) {
    return translateExceptions(() -> Futures.getUnchecked(searchAsync(searchParam)));
  }

  @Override
  public ListenableFuture<SearchResult> searchAsync(@Nonnull SearchParam searchParam) {
    return translateExceptions(
        () -> {
          io.milvus.grpc.SearchParam request = searchParam.grpc();
          ListenableFuture<QueryResult> responseFuture = futureStub().search(request);
          return Futures.transform(
              responseFuture,
              queryResult -> {
                checkResponseStatus(queryResult.getStatus());
                return buildSearchResponse(queryResult);
              },
              MoreExecutors.directExecutor());
        });
  }

  @Override
  public CollectionMapping getCollectionInfo(@Nonnull String collectionName) {
    return translateExceptions(
        () -> {
          CollectionName request =
              CollectionName.newBuilder().setCollectionName(collectionName).build();
          Mapping response = blockingStub().describeCollection(request);
          checkResponseStatus(response.getStatus());
          return new CollectionMapping(response);
        });
  }

  @Override
  public List<String> listCollections() {
    return translateExceptions(
        () -> {
          Command request = Command.newBuilder().setCmd("").build();
          CollectionNameList response = blockingStub().showCollections(request);
          checkResponseStatus(response.getStatus());
          return response.getCollectionNamesList();
        });
  }

  @Override
  public long countEntities(@Nonnull String collectionName) {
    return translateExceptions(
        () -> {
          CollectionName request =
              CollectionName.newBuilder().setCollectionName(collectionName).build();
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
    return translateExceptions(
        () -> {
          Command request = Command.newBuilder().setCmd(command).build();
          StringReply response = blockingStub().cmd(request);
          checkResponseStatus(response.getStatus());
          return response.getStringReply();
        });
  }

  @Override
  public void loadCollection(@Nonnull String collectionName) {
    translateExceptions(
        () -> {
          CollectionName request =
              CollectionName.newBuilder().setCollectionName(collectionName).build();
          Status response = blockingStub().preloadCollection(request);
          checkResponseStatus(response);
        });
  }

  @Override
  public void dropIndex(String collectionName, String fieldName) {
    translateExceptions(
        () -> {
          IndexParam request =
              IndexParam.newBuilder()
                  .setCollectionName(collectionName)
                  .setFieldName(fieldName)
                  .build();
          Status response = blockingStub().dropIndex(request);
          checkResponseStatus(response);
        });
  }

  @Override
  public String getCollectionStats(String collectionName) {
    return translateExceptions(
        () -> {
          CollectionName request =
              CollectionName.newBuilder().setCollectionName(collectionName).build();
          CollectionInfo response = blockingStub().showCollectionInfo(request);
          checkResponseStatus(response.getStatus());
          return response.getJsonInfo();
        });
  }

  @Override
  public Map<Long, Map<String, Object>> getEntityByID(
      String collectionName, List<Long> ids, List<String> fieldNames) {
    return translateExceptions(
        () -> {
          EntityIdentity request =
              EntityIdentity.newBuilder()
                  .setCollectionName(collectionName)
                  .addAllIdArray(ids)
                  .addAllFieldNames(fieldNames)
                  .build();
          Entities response = blockingStub().getEntityByID(request);
          checkResponseStatus(response.getStatus());
          Map<String, Iterator<?>> fieldIterators =
              response.getFieldsList().stream()
                  .collect(Collectors.toMap(FieldValue::getFieldName, this::fieldValueIterator));
          Iterator<Long> idIterator = ids.iterator();
          Map<Long, Map<String, Object>> entities =
              new HashMap<>(response.getValidRowList().size());
          for (boolean valid : response.getValidRowList()) {
            long id = idIterator.next();
            if (valid) {
              entities.put(id, toMap(fieldIterators));
            }
          }
          return entities;
        });
  }

  private Map<String, Object> toMap(Map<String, Iterator<?>> fieldIterators) {
    return fieldIterators.entrySet().stream()
        .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue().next()));
  }

  private Iterator<?> fieldValueIterator(FieldValue fieldValue) {
    if (fieldValue.hasAttrRecord()) {
      AttrRecord record = fieldValue.getAttrRecord();
      if (record.getInt32ValueCount() > 0) {
        return record.getInt32ValueList().iterator();
      } else if (record.getInt64ValueCount() > 0) {
        return record.getInt64ValueList().iterator();
      } else if (record.getFloatValueCount() > 0) {
        return record.getFloatValueList().iterator();
      } else if (record.getDoubleValueCount() > 0) {
        return record.getDoubleValueList().iterator();
      }
    }
    VectorRecord record = fieldValue.getVectorRecord();
    return record.getRecordsList().stream()
        .map(
            row ->
                row.getFloatDataCount() > 0
                    ? row.getFloatDataList()
                    : row.getBinaryData().asReadOnlyByteBuffer())
        .iterator();
  }

  @Override
  public Map<Long, Map<String, Object>> getEntityByID(String collectionName, List<Long> ids) {
    return getEntityByID(collectionName, ids, Collections.emptyList());
  }

  @Override
  public List<Long> listIDInSegment(String collectionName, Long segmentId) {
    return translateExceptions(
        () -> {
          GetEntityIDsParam request =
              GetEntityIDsParam.newBuilder()
                  .setCollectionName(collectionName)
                  .setSegmentId(segmentId)
                  .build();
          EntityIds response = blockingStub().getEntityIDs(request);
          checkResponseStatus(response.getStatus());
          return response.getEntityIdArrayList();
        });
  }

  @Override
  public void deleteEntityByID(String collectionName, List<Long> ids) {
    translateExceptions(
        () -> {
          DeleteByIDParam request =
              DeleteByIDParam.newBuilder()
                  .setCollectionName(collectionName)
                  .addAllIdArray(ids)
                  .build();
          Status response = blockingStub().deleteByID(request);
          checkResponseStatus(response);
        });
  }

  @Override
  public void flush(List<String> collectionNames) {
    translateExceptions(() -> Futures.getUnchecked(flushAsync(collectionNames)));
  }

  @Override
  public ListenableFuture<Void> flushAsync(@Nonnull List<String> collectionNames) {
    return translateExceptions(
        () -> {
          FlushParam request =
              FlushParam.newBuilder().addAllCollectionNameArray(collectionNames).build();
          ListenableFuture<Status> response = futureStub().flush(request);
          return Futures.transform(
              response, this::checkResponseStatus, MoreExecutors.directExecutor());
        });
  }

  @Override
  public void flush(String collectionName) {
    flush(Collections.singletonList(collectionName));
  }

  @Override
  public ListenableFuture<Void> flushAsync(String collectionName) {
    return flushAsync(Collections.singletonList(collectionName));
  }

  @Override
  public void compact(CompactParam compactParam) {
    translateExceptions(() -> Futures.getUnchecked(compactAsync(compactParam)));
  }

  @Override
  public ListenableFuture<Void> compactAsync(@Nonnull CompactParam compactParam) {
    return translateExceptions(
        () -> {
          io.milvus.grpc.CompactParam request = compactParam.grpc();
          ListenableFuture<Status> response = futureStub().compact(request);
          return Futures.transform(
              response, this::checkResponseStatus, MoreExecutors.directExecutor());
        });
  }

  ///////////////////// Util Functions/////////////////////
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
          if (fieldValue.getAttrRecord().getInt32ValueCount() > j) {
            fieldsMap.get(j).put(fieldName, fieldValue.getAttrRecord().getInt32ValueList().get(j));
          } else if (fieldValue.getAttrRecord().getInt64ValueCount() > j) {
            fieldsMap.get(j).put(fieldName, fieldValue.getAttrRecord().getInt64ValueList().get(j));
          } else if (fieldValue.getAttrRecord().getDoubleValueCount() > j) {
            fieldsMap.get(j).put(fieldName, fieldValue.getAttrRecord().getDoubleValueList().get(j));
          } else if (fieldValue.getAttrRecord().getFloatValueCount() > j) {
            fieldsMap.get(j).put(fieldName, fieldValue.getAttrRecord().getFloatValueList().get(j));
          } else {
            // the object is vector
            List<VectorRowRecord> vectorRowRecordList =
                fieldValue.getVectorRecord().getRecordsList();
            if (vectorRowRecordList.size() > j) {
              if (vectorRowRecordList.get(j).getFloatDataCount() > 0) {
                fieldsMap.get(j).put(fieldName, vectorRowRecordList.get(j).getFloatDataList());
              } else {
                fieldsMap
                    .get(j)
                    .put(
                        fieldName,
                        vectorRowRecordList.get(j).getBinaryData().asReadOnlyByteBuffer());
              }
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
}
