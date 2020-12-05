package io.milvus.client.dsl;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.milvus.client.CollectionMapping;
import io.milvus.client.CompactParam;
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

/**
 * A service that wraps client, collection name and schema together to simplify API calls. It is
 * recommended to use the service if you need a schema for Milvus operations. All operations can
 * then be called with <code>MilvusService</code> instead of <code>MilvusClient</code>.
 */
public class MilvusService {
  private final MilvusClient client;
  private final String collectionName;
  private final Schema schema;

  public MilvusService(MilvusClient client, String collectionName, Schema schema) {
    this.client = client;
    this.collectionName = collectionName;
    this.schema = schema;
  }

  /**
   * Milvus service with timeout support.
   *
   * @param timeout the desired timeout
   * @param timeoutUnit unit for timeout
   */
  public MilvusService withTimeout(int timeout, TimeUnit timeoutUnit) {
    return new MilvusService(client.withTimeout(timeout, timeoutUnit), collectionName, schema);
  }

  /** Close the client. Wait at most 1 minute for graceful shutdown. */
  public void close() {
    client.close();
  }

  /** Count entities in the current collection */
  public long countEntities() {
    return client.countEntities(collectionName);
  }

  /** Create collection with predefined schema. */
  public void createCollection() {
    createCollection("{}");
  }

  /**
   * Create collection with predefined schema.
   *
   * @param paramsInJson Set optional "segment_row_limit" or "auto_id".
   */
  public void createCollection(String paramsInJson) {
    client.createCollection(schema.mapToCollection(collectionName).setParamsInJson(paramsInJson));
  }

  /**
   * Create index with schema field.
   *
   * <pre>
   * example usage:
   * <code>
   * service.createIndex(
   *     schema.embedding,
   *     IndexType.IVF_FLAT,
   *     MetricType.L2,
   *     new JsonBuilder().param("nlist", 100).build());
   * </code>
   * </pre>
   */
  public void createIndex(
      Schema.VectorField vectorField,
      IndexType indexType,
      MetricType metricType,
      String paramsInJson) {
    Futures.getUnchecked(createIndexAsync(vectorField, indexType, metricType, paramsInJson));
  }

  /**
   * Create index with schema field.
   *
   * <pre>
   * example usage:
   * <code>
   * service.createIndex(
   *     schema.embedding,
   *     IndexType.IVF_FLAT,
   *     MetricType.L2,
   *     new JsonBuilder().param("nlist", 100).build());
   * </code>
   * </pre>
   */
  public ListenableFuture<Void> createIndexAsync(
      Schema.VectorField vectorField,
      IndexType indexType,
      MetricType metricType,
      String paramsInJson) {
    return client.createIndexAsync(
        Index.create(collectionName, vectorField.name)
            .setIndexType(indexType)
            .setMetricType(metricType)
            .setParamsInJson(paramsInJson));
  }

  /** Delete entities by IDs. */
  public void deleteEntityByID(List<Long> ids) {
    client.deleteEntityByID(collectionName, ids);
  }

  /** Drop the current collection. */
  public void dropCollection() {
    client.dropCollection(collectionName);
  }

  /** Flush the current collection. */
  public void flush() {
    client.flush(collectionName);
  }

  /** Flush the current collection. */
  public ListenableFuture<Void> flushAsync() {
    return client.flushAsync(collectionName);
  }

  /**
   * Get entity by IDs.
   *
   * @param ids The ids to get
   * @return Map of id to <code>Schema.Entity</code> value.
   */
  public Map<Long, Schema.Entity> getEntityByID(List<Long> ids) {
    return getEntityByID(ids, Collections.emptyList());
  }

  /**
   * Get entity by IDs.
   *
   * @param ids The ids to get
   * @param fields The fields to return values for
   * @return Map of id to <code>Schema.Entity</code> value.
   */
  public Map<Long, Schema.Entity> getEntityByID(List<Long> ids, List<Schema.Field<?>> fields) {
    List<String> fieldNames = fields.stream().map(f -> f.name).collect(Collectors.toList());
    return client.getEntityByID(collectionName, ids, fieldNames).entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey(), e -> schema.new Entity(e.getValue())));
  }

  /**
   * Check whether a collection exists
   *
   * @param collectionName collection to check
   * @return true if the collection exists, false otherwise.
   */
  public boolean hasCollection(String collectionName) {
    return client.hasCollection(collectionName);
  }

  /**
   * Insert data with schema.
   *
   * <pre>
   * example usage:
   * <code>
   * service.insert(
   *         insertParam ->
   *             insertParam
   *                 .withIds(ids)
   *                 .with(schema.int_field, int_values)
   *                 .with(schema.embedding, embeddings));
   * </code>
   * </pre>
   *
   * @return a list of ids of the inserted entities
   */
  public List<Long> insert(Consumer<InsertParam> insertParamBuilder) {
    return Futures.getUnchecked(insertAsync(insertParamBuilder));
  }

  /**
   * Insert data with schema.
   *
   * <pre>
   * example usage:
   * <code>
   * service.insert(
   *         insertParam ->
   *             insertParam
   *                 .withIds(ids)
   *                 .with(schema.int_field, int_values)
   *                 .with(schema.embedding, embeddings));
   * </code>
   * </pre>
   *
   * @return a list of ids of the inserted entities
   */
  public ListenableFuture<List<Long>> insertAsync(Consumer<InsertParam> insertParamBuilder) {
    InsertParam insertParam = schema.insertInto(collectionName);
    insertParamBuilder.accept(insertParam);
    return client.insertAsync(insertParam.getInsertParam());
  }

  /** Search with searchParam. */
  public SearchResult search(SearchParam searchParam) {
    return client.search(searchParam);
  }

  /** Search with searchParam. */
  public ListenableFuture<SearchResult> searchAsync(SearchParam searchParam) {
    return client.searchAsync(searchParam);
  }

  /**
   * Build a SearchParam from Query.
   *
   * <pre>
   * example usage:
   * <code>
   * SearchParam searchParam =
   *     service.buildSearchParam(query)
   *            .setParamsInJson("{\"fields\": [\"A\", \"B\"]}");
   * </code>
   * </pre>
   */
  public SearchParam buildSearchParam(Query query) {
    return query.buildSearchParam(collectionName);
  }

  /**
   * Create a partition specified by <code>tag</code>
   *
   * @param tag partition tag
   */
  public void createPartition(String tag) {
    client.createPartition(collectionName, tag);
  }

  /**
   * Check whether the partition exists in this collection
   *
   * @param tag partition tag
   * @return true if the partition exists, false otherwise.
   */
  public boolean hasPartition(String tag) {
    return client.hasPartition(collectionName, tag);
  }

  /**
   * List current partitions in the collection
   *
   * @return a list of partition names
   */
  public List<String> listPartitions() {
    return client.listPartitions(collectionName);
  }

  /**
   * Drop partition in the collection specified by <code>tag</code>
   *
   * @param tag partition tag
   */
  public void dropPartition(String tag) {
    client.dropPartition(collectionName, tag);
  }

  /** Get current collection info */
  public CollectionMapping getCollectionInfo() {
    return client.getCollectionInfo(collectionName);
  }

  /** List collections. */
  public List<String> listCollections() {
    return client.listCollections();
  }

  /** Drop index by schema. */
  public void dropIndex(Schema.VectorField vectorField) {
    client.dropIndex(collectionName, vectorField.name);
  }

  /** Get collection stats. */
  public String getCollectionStats() {
    return client.getCollectionStats(collectionName);
  }

  /**
   * Compacts the collection, erasing deleted data from disk and rebuild index in background (if the
   * data size after compaction is still larger than indexFileSize). Data was only soft-deleted
   * until you call compact.
   */
  public void compact() {
    client.compact(CompactParam.create(collectionName));
  }

  /**
   * Compacts the collection, erasing deleted data from disk and rebuild index in background (if the
   * data size after compaction is still larger than indexFileSize). Data was only soft-deleted
   * until you call compact.
   */
  public ListenableFuture<Void> compactAsync() {
    return client.compactAsync(CompactParam.create(collectionName));
  }

  /**
   * Compacts the collection, erasing deleted data from disk and rebuild index in background (if the
   * data size after compaction is still larger than indexFileSize). Data was only soft-deleted
   * until you call compact.
   *
   * @param threshold Defaults to 0.2. Segment will compact if and only if the percentage of
   *     entities deleted exceeds the threshold.
   */
  public void compact(double threshold) {
    client.compact(CompactParam.create(collectionName).setThreshold(threshold));
  }

  /**
   * Compacts the collection, erasing deleted data from disk and rebuild index in background (if the
   * data size after compaction is still larger than indexFileSize). Data was only soft-deleted
   * until you call compact.
   *
   * @param threshold Defaults to 0.2. Segment will compact if and only if the percentage of
   *     entities deleted exceeds the threshold.
   */
  public ListenableFuture<Void> compactAsync(double threshold) {
    return client.compactAsync(CompactParam.create(collectionName).setThreshold(threshold));
  }
}
