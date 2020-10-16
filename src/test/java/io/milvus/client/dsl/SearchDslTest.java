package io.milvus.client.dsl;

import io.milvus.client.ConnectParam;
import io.milvus.client.IndexType;
import io.milvus.client.JsonBuilder;
import io.milvus.client.MetricType;
import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusGrpcClient;
import io.milvus.client.SearchParam;
import io.milvus.client.SearchResult;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class SearchDslTest {

  @Container
  private GenericContainer milvusContainer =
      new GenericContainer(System.getProperty("docker_image_name", "milvusdb/milvus:0.11.0-cpu"))
          .withExposedPorts(19530);

  private TestSchema schema = new TestSchema();
  private String collectionName = "test_collection";
  private int size = 1000;

  private ConnectParam connectParam(GenericContainer milvusContainer) {
    return new ConnectParam.Builder()
        .withHost(milvusContainer.getHost())
        .withPort(milvusContainer.getFirstMappedPort())
        .build();
  }

  private void withMilvusService(Consumer<MilvusService> test) {
    try (MilvusClient client = new MilvusGrpcClient(connectParam(milvusContainer))) {
      test.accept(new MilvusService(client, collectionName, schema));
    }
  }

  private List<Float> randomFloatVector(int dimension) {
    return Stream.generate(RandomUtils::nextFloat).limit(dimension).collect(Collectors.toList());
  }

  private List<List<Float>> randomFloatVectors(int size, int dimension) {
    return Stream.generate(() -> randomFloatVector(dimension)).limit(size).collect(Collectors.toList());
  }

  private ByteBuffer randomBinaryVector(int dimension) {
    return ByteBuffer.wrap(RandomUtils.nextBytes(dimension / 8));
  }

  private List<ByteBuffer> randomBinaryVectors(int size, int dimension) {
    return Stream.generate(() -> randomBinaryVector(dimension)).limit(size).collect(Collectors.toList());
  }

  @Test
  public void testCreateCollection() {
    withMilvusService(service -> {
      service.createCollection(new JsonBuilder().param("auto_id", false).build());
      assertTrue(service.hasCollection(collectionName));
    });
  }

  @Test
  public void testInsert() {
    testCreateCollection();

    withMilvusService(service -> {
      service.insert(insertParam -> insertParam
          .withIds(LongStream.range(0, size).boxed().collect(Collectors.toList()))
          .with(schema.intField, IntStream.range(0, size).boxed().collect(Collectors.toList()))
          .with(schema.longField, LongStream.range(0, size).boxed().collect(Collectors.toList()))
          .with(schema.floatField, IntStream.range(0, size).boxed().map(Number::floatValue).collect(Collectors.toList()))
          .with(schema.doubleField, IntStream.range(0, size).boxed().map(Number::doubleValue).collect(Collectors.toList()))
          .with(schema.floatVectorField, randomFloatVectors(size, schema.floatVectorField.dimension))
          .with(schema.binaryVectorField, randomBinaryVectors(size, schema.binaryVectorField.dimension)));

      service.flush();

      assertEquals(size, service.countEntities());
    });
  }

  @Test
  public void testCreateIndex() {
    testInsert();

    withMilvusService(service -> {
      service.createIndex(schema.floatVectorField, IndexType.IVF_SQ8, MetricType.L2, "{\"nlist\": 256}");
      service.createIndex(schema.binaryVectorField, IndexType.BIN_FLAT, MetricType.JACCARD, "{}");
    });
  }

  @Test
  public void testGetEntityById() {
    withMilvusService(service -> {
      testInsert();

      Map<Long, Schema.Entity> entities = service.getEntityByID(
          LongStream.range(0, 10).boxed().collect(Collectors.toList()),
          Arrays.asList(schema.intField, schema.longField));

      LongStream.range(0, 10).forEach(i -> {
        assertEquals((int) i, entities.get(i).get(schema.intField));
        assertEquals(i, entities.get(i).get(schema.longField));
      });
    });
  }

  @Test
  public void testFloadVectorQuery() {
    withMilvusService(service -> {
      testCreateIndex();

      List<Long> entityIds = LongStream.range(0, 10).boxed().collect(Collectors.toList());

      Map<Long, Schema.Entity> entities = service.getEntityByID(entityIds);

      List<List<Float>> vectors = entities.values().stream().map(e -> e.get(schema.floatVectorField)).collect(Collectors.toList());

      Query query = Query.bool(
          Query.must(
              schema.floatVectorField.query(vectors).param("nprobe", 16).top(1)
          )
      );

      SearchParam searchParam = service.buildSearchParam(query)
          .setParamsInJson(new JsonBuilder().param("fields", Arrays.asList("int64", "float_vec")).build());

      SearchResult searchResult = service.search(searchParam);
      assertEquals(entityIds,
          searchResult.getResultIdsList().stream()
              .map(ids -> ids.get(0))
              .collect(Collectors.toList()));
    });
  }

  @Test
  public void testBinaryVectorQuery() {
    withMilvusService(service -> {
      testCreateIndex();

      List<Long> entityIds = LongStream.range(0, 10).boxed().collect(Collectors.toList());

      Map<Long, Schema.Entity> entities = service.getEntityByID(entityIds);

      List<ByteBuffer> vectors = entities.values().stream().map(e -> e.get(schema.binaryVectorField)).collect(Collectors.toList());

      Query query = Query.bool(
          Query.must(
              schema.binaryVectorField.query(vectors).top(1)
          )
      );

      SearchParam searchParam = service.buildSearchParam(query);

      SearchResult searchResult = service.search(searchParam);
      assertEquals(entityIds,
          searchResult.getResultIdsList().stream()
              .map(ids -> ids.get(0))
              .collect(Collectors.toList()));
    });
  }
}
