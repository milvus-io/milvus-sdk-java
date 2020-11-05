package io.milvus.client.dsl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.milvus.client.ConnectParam;
import io.milvus.client.IndexType;
import io.milvus.client.JsonBuilder;
import io.milvus.client.MetricType;
import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusGrpcClient;
import io.milvus.client.SearchParam;
import io.milvus.client.SearchResult;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class SearchDslTest {

  @Container
  private final GenericContainer milvusContainer =
      new GenericContainer(
              System.getProperty("docker_image_name", "milvusdb/milvus:0.11.0-cpu-d101620-4c44c0"))
          .withExposedPorts(19530);

  private final TestFloatSchema floatSchema = new TestFloatSchema();
  private final TestBinarySchema binarySchema = new TestBinarySchema();
  private final String collectionName = "test_collection";
  private final int size = 1000;

  private ConnectParam connectParam(GenericContainer milvusContainer) {
    return new ConnectParam.Builder()
        .withHost(milvusContainer.getHost())
        .withPort(milvusContainer.getFirstMappedPort())
        .build();
  }

  private void withMilvusServiceFloat(Consumer<MilvusService> test) {
    try (MilvusClient client = new MilvusGrpcClient(connectParam(milvusContainer))) {
      test.accept(new MilvusService(client, collectionName, floatSchema));
    }
  }

  private void withMilvusServiceBinary(Consumer<MilvusService> test) {
    try (MilvusClient client = new MilvusGrpcClient(connectParam(milvusContainer))) {
      test.accept(new MilvusService(client, collectionName, binarySchema));
    }
  }

  private List<Float> randomFloatVector(int dimension) {
    return Stream.generate(RandomUtils::nextFloat).limit(dimension).collect(Collectors.toList());
  }

  private List<List<Float>> randomFloatVectors(int size, int dimension) {
    return Stream.generate(() -> randomFloatVector(dimension))
        .limit(size)
        .collect(Collectors.toList());
  }

  private ByteBuffer randomBinaryVector(int dimension) {
    return ByteBuffer.wrap(RandomUtils.nextBytes(dimension / 8));
  }

  private List<ByteBuffer> randomBinaryVectors(int size, int dimension) {
    return Stream.generate(() -> randomBinaryVector(dimension))
        .limit(size)
        .collect(Collectors.toList());
  }

  @Test
  public void testCreateCollectionFloat() {
    withMilvusServiceFloat(
        service -> {
          service.createCollection(new JsonBuilder().param("auto_id", false).build());
          assertTrue(service.hasCollection(collectionName));
        });
  }

  @Test
  public void testCreateCollectionBinary() {
    withMilvusServiceBinary(
        service -> {
          service.createCollection(new JsonBuilder().param("auto_id", false).build());
          assertTrue(service.hasCollection(collectionName));
        });
  }

  @Test
  public void testInsertFloat() {
    testCreateCollectionFloat();

    withMilvusServiceFloat(
        service -> {
          service.insert(
              insertParam ->
                  insertParam
                      .withIds(LongStream.range(0, size).boxed().collect(Collectors.toList()))
                      .with(
                          floatSchema.intField,
                          IntStream.range(0, size).boxed().collect(Collectors.toList()))
                      .with(
                          floatSchema.longField,
                          LongStream.range(0, size).boxed().collect(Collectors.toList()))
                      .with(
                          floatSchema.floatField,
                          IntStream.range(0, size)
                              .boxed()
                              .map(Number::floatValue)
                              .collect(Collectors.toList()))
                      .with(
                          floatSchema.doubleField,
                          IntStream.range(0, size)
                              .boxed()
                              .map(Number::doubleValue)
                              .collect(Collectors.toList()))
                      .with(
                          floatSchema.floatVectorField,
                          randomFloatVectors(size, floatSchema.floatVectorField.dimension)));

          service.flush();

          assertEquals(size, service.countEntities());
        });
  }

  @Test
  public void testInsertBinary() {
    testCreateCollectionBinary();

    withMilvusServiceBinary(
        service -> {
          service.insert(
              insertParam ->
                  insertParam
                      .withIds(LongStream.range(0, size).boxed().collect(Collectors.toList()))
                      .with(
                          binarySchema.intField,
                          IntStream.range(0, size).boxed().collect(Collectors.toList()))
                      .with(
                          binarySchema.longField,
                          LongStream.range(0, size).boxed().collect(Collectors.toList()))
                      .with(
                          binarySchema.floatField,
                          IntStream.range(0, size)
                              .boxed()
                              .map(Number::floatValue)
                              .collect(Collectors.toList()))
                      .with(
                          binarySchema.doubleField,
                          IntStream.range(0, size)
                              .boxed()
                              .map(Number::doubleValue)
                              .collect(Collectors.toList()))
                      .with(
                          binarySchema.binaryVectorField,
                          randomBinaryVectors(size, binarySchema.binaryVectorField.dimension)));

          service.flush();

          assertEquals(size, service.countEntities());
        });
  }

  @Test
  public void testCreateIndexFloat() {
    testInsertFloat();

    withMilvusServiceFloat(
        service -> {
          service.createIndex(
              floatSchema.floatVectorField, IndexType.IVF_SQ8, MetricType.L2, "{\"nlist\": 256}");
        });
  }

  @Test
  public void testCreateIndexBinary() {
    testInsertBinary();

    withMilvusServiceBinary(
        service -> {
          service.createIndex(
              binarySchema.binaryVectorField, IndexType.BIN_FLAT, MetricType.JACCARD, "{}");
        });
  }

  @Test
  public void testGetEntityByIdFloat() {
    withMilvusServiceFloat(
        service -> {
          testInsertFloat();

          Map<Long, Schema.Entity> entities =
              service.getEntityByID(
                  LongStream.range(0, 10).boxed().collect(Collectors.toList()),
                  Arrays.asList(floatSchema.intField, floatSchema.longField));

          LongStream.range(0, 10)
              .forEach(
                  i -> {
                    assertEquals((int) i, entities.get(i).get(floatSchema.intField));
                    assertEquals(i, entities.get(i).get(floatSchema.longField));
                  });
        });
  }

  @Test
  public void testGetEntityByIdBinary() {
    withMilvusServiceBinary(
        service -> {
          testInsertBinary();

          Map<Long, Schema.Entity> entities =
              service.getEntityByID(
                  LongStream.range(0, 10).boxed().collect(Collectors.toList()),
                  Arrays.asList(binarySchema.intField, binarySchema.longField));

          LongStream.range(0, 10)
              .forEach(
                  i -> {
                    assertEquals((int) i, entities.get(i).get(binarySchema.intField));
                    assertEquals(i, entities.get(i).get(binarySchema.longField));
                  });
        });
  }

  @Test
  public void testFloatVectorQuery() {
    withMilvusServiceFloat(
        service -> {
          testCreateIndexFloat();

          List<Long> entityIds = LongStream.range(0, 10).boxed().collect(Collectors.toList());

          Map<Long, Schema.Entity> entities = service.getEntityByID(entityIds);

          List<List<Float>> vectors =
              entities.values().stream()
                  .map(e -> e.get(floatSchema.floatVectorField))
                  .collect(Collectors.toList());

          Query query =
              Query.bool(
                  Query.must(
                      floatSchema.floatVectorField.query(vectors).param("nprobe", 16).top(1)));

          SearchParam searchParam =
              service
                  .buildSearchParam(query)
                  .setParamsInJson(
                      new JsonBuilder()
                          .param("fields", Arrays.asList("int64", "float_vec"))
                          .build());

          SearchResult searchResult = service.search(searchParam);
          assertEquals(
              entityIds,
              searchResult.getResultIdsList().stream()
                  .map(ids -> ids.get(0))
                  .collect(Collectors.toList()));
        });
  }

  @Test
  public void testBinaryVectorQuery() {
    withMilvusServiceBinary(
        service -> {
          testCreateIndexBinary();

          List<Long> entityIds = LongStream.range(0, 10).boxed().collect(Collectors.toList());

          Map<Long, Schema.Entity> entities = service.getEntityByID(entityIds);

          List<ByteBuffer> vectors =
              entities.values().stream()
                  .map(e -> e.get(binarySchema.binaryVectorField))
                  .collect(Collectors.toList());

          Query query =
              Query.bool(Query.must(binarySchema.binaryVectorField.query(vectors).top(1)));

          SearchParam searchParam = service.buildSearchParam(query);

          SearchResult searchResult = service.search(searchParam);
          assertEquals(
              entityIds,
              searchResult.getResultIdsList().stream()
                  .map(ids -> ids.get(0))
                  .collect(Collectors.toList()));
        });
  }
}
