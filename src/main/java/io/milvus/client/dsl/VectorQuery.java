package io.milvus.client.dsl;

import com.google.protobuf.UnsafeByteOperations;
import io.milvus.client.MetricType;
import io.milvus.client.SearchParam;
import io.milvus.grpc.VectorParam;
import io.milvus.grpc.VectorRecord;
import io.milvus.grpc.VectorRowRecord;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import org.json.JSONObject;

public class VectorQuery<T> extends Query {
  private final Schema.VectorField<T> field;
  private final List<T> queries;
  private String placeholder;
  private int topK = 10;
  private float boost = 1.0f;
  private MetricType metricType;
  private JSONObject params = new JSONObject();

  VectorQuery(Schema.VectorField<T> field, List<T> queries) {
    this.field = field;
    this.queries = queries;
    this.placeholder = field.name;
    this.metricType = field instanceof Schema.FloatVectorField ? MetricType.L2 : MetricType.JACCARD;
  }

  public VectorQuery<T> placeholder(String placeholder) {
    this.placeholder = placeholder;
    return this;
  }

  public VectorQuery<T> top(int topK) {
    this.topK = topK;
    return this;
  }

  public VectorQuery<T> boost(float value) {
    this.boost = value;
    return this;
  }

  public VectorQuery<T> metricType(MetricType metricType) {
    this.metricType = metricType;
    return this;
  }

  public VectorQuery<T> param(String key, Object value) {
    params.put(key, value);
    return this;
  }

  public VectorQuery<T> paramsInJson(String paramsInJson) {
    params = new JSONObject(paramsInJson);
    return this;
  }

  @SuppressWarnings("unchecked")
  void buildSearchParam(SearchParam searchParam) {
    VectorRecord vectorRecord = null;
    if (field instanceof Schema.FloatVectorField) {
      vectorRecord =
          VectorRecord.newBuilder()
              .addAllRecords(
                  ((List<List<Float>>) this.queries)
                      .stream()
                          .map(
                              vector ->
                                  VectorRowRecord.newBuilder().addAllFloatData(vector).build())
                          .collect(Collectors.toList()))
              .build();
    } else if (field instanceof Schema.BinaryVectorField) {
      vectorRecord =
          VectorRecord.newBuilder()
              .addAllRecords(
                  ((List<ByteBuffer>) this.queries)
                      .stream()
                          .map(
                              vector ->
                                  VectorRowRecord.newBuilder()
                                      .setBinaryData(UnsafeByteOperations.unsafeWrap(vector))
                                      .build())
                          .collect(Collectors.toList()))
              .build();
    }

    VectorParam vectorParam =
        VectorParam.newBuilder()
            .setJson(
                new JSONObject()
                    .put(
                        placeholder,
                        new JSONObject()
                            .put(
                                field.name,
                                new JSONObject()
                                    .put("topk", topK)
                                    .put("metric_type", metricType.name())
                                    .put("boost", boost)
                                    .put("params", params)))
                    .toString())
            .setRowRecord(vectorRecord)
            .build();

    searchParam.addQueries(vectorParam);
  }

  @Override
  protected JSONObject buildSearchParam(SearchParam searchParam, JSONObject outer) {
    buildSearchParam(searchParam);
    return outer.put("vector", placeholder);
  }
}
