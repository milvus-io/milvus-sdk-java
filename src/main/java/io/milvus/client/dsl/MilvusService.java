package io.milvus.client.dsl;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.milvus.client.Index;
import io.milvus.client.IndexType;
import io.milvus.client.MetricType;
import io.milvus.client.MilvusClient;
import io.milvus.client.SearchParam;
import io.milvus.client.SearchResult;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class MilvusService {
  private final MilvusClient client;
  private final String collectionName;
  private final Schema schema;

  public MilvusService(MilvusClient client, String  collectionName, Schema schema) {
    this.client = client;
    this.collectionName = collectionName;
    this.schema = schema;
  }

  public MilvusService withTimeout(int timeout, TimeUnit unit) {
    return new MilvusService(client.withTimeout(timeout, unit), collectionName, schema);
  }

  public void close() {
    client.close();
  }

  public long countEntities() {
    return client.countEntities(collectionName);
  }

  public void createCollection() {
    createCollection("{}");
  }

  public void createCollection(String paramsInJson) {
    client.createCollection(schema.mapToCollection(collectionName).setParamsInJson(paramsInJson));
  }

  public void createIndex(
      Schema.VectorField vectorField, IndexType indexType, MetricType metricType, String paramsInJson) {
    Futures.getUnchecked(createIndexAsync(vectorField, indexType, metricType, paramsInJson));
  }

  public ListenableFuture<Void> createIndexAsync(
      Schema.VectorField vectorField, IndexType indexType, MetricType metricType, String paramsInJson) {
    return client.createIndexAsync(
        Index.create(collectionName, vectorField.name)
            .setIndexType(indexType)
            .setMetricType(metricType)
            .setParamsInJson(paramsInJson));
  }

  public void deleteEntityByID(List<Long> ids) {
    client.deleteEntityByID(collectionName, ids);
  }

  public void dropCollection() {
    client.dropCollection(collectionName);
  }

  public void flush() {
    client.flush(collectionName);
  }

  public ListenableFuture<Void> flushAsync() {
    return client.flushAsync(collectionName);
  }

  public Map<Long, Schema.Entity> getEntityByID(List<Long> ids) {
    return getEntityByID(ids, Collections.emptyList());
  }

  public Map<Long, Schema.Entity> getEntityByID(List<Long> ids, List<Schema.Field<?>> fields) {
    List<String> fieldNames = fields.stream().map(f -> f.name).collect(Collectors.toList());
    return client.getEntityByID(collectionName, ids, fieldNames)
        .entrySet().stream().collect(Collectors.toMap(
            e -> e.getKey(),
            e -> schema.new Entity(e.getValue())));
  }

  public boolean hasCollection(String collectionName) {
    return client.hasCollection(collectionName);
  }

  public List<Long> insert(Consumer<InsertParam> insertParamBuilder) {
    return Futures.getUnchecked(insertAsync(insertParamBuilder));
  }

  public ListenableFuture<List<Long>> insertAsync(Consumer<InsertParam> insertParamBuilder) {
    InsertParam insertParam = schema.insertInto(collectionName);
    insertParamBuilder.accept(insertParam);
    return client.insertAsync(insertParam.getInsertParam());
  }

  public SearchResult search(SearchParam searchParam) {
    return client.search(searchParam);
  }

  public ListenableFuture<SearchResult> searchAsync(SearchParam searchParam) {
    return client.searchAsync(searchParam);
  }

  public SearchParam buildSearchParam(Query query) {
    return query.buildSearchParam(collectionName);
  }
}
