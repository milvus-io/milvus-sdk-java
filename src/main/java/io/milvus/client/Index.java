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

import io.milvus.grpc.IndexParam;
import io.milvus.grpc.KeyValuePair;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Represents an index containing <code>fieldName</code>, <code>indexName</code> and <code>
 * paramsInJson</code>, which contains <code>index_type</code>, params etc.
 */
public class Index {
  private final IndexParam.Builder builder;

  private Index(String collectionName, String fieldName) {
    this.builder =
        IndexParam.newBuilder().setCollectionName(collectionName).setFieldName(fieldName);
  }

  /**
   * @param collectionName collection to create index for
   * @param fieldName name of the field on which index is built.
   */
  public static Index create(@Nonnull String collectionName, @Nonnull String fieldName) {
    return new Index(collectionName, fieldName);
  }

  public String getCollectionName() {
    return builder.getCollectionName();
  }

  /** @param collectionName The collection name */
  public Index setCollectionName(@Nonnull String collectionName) {
    builder.setCollectionName(collectionName);
    return this;
  }

  public String getFieldName() {
    return builder.getFieldName();
  }

  /** @param fieldName The field name */
  public Index setFieldName(@Nonnull String fieldName) {
    builder.setFieldName(fieldName);
    return this;
  }

  public String getIndexName() {
    return builder.getIndexName();
  }

  public Map<String, String> getExtraParams() {
    return toMap(builder.getExtraParamsList());
  }

  /** @param indexType The index type */
  public Index setIndexType(IndexType indexType) {
    return addParam("index_type", indexType.name());
  }

  /** @param metricType The metric type */
  public Index setMetricType(MetricType metricType) {
    return addParam("metric_type", metricType.name());
  }

  /** @param paramsInJson optional parameters for index, such as <code>nlist</code> */
  public Index setParamsInJson(String paramsInJson) {
    return addParam(MilvusClient.extraParamKey, paramsInJson);
  }

  private Index addParam(String key, Object value) {
    builder.addExtraParams(
        KeyValuePair.newBuilder().setKey(key).setValue(String.valueOf(value)).build());
    return this;
  }

  @Override
  public String toString() {
    return "Index {"
        + "collectionName="
        + getCollectionName()
        + ", fieldName="
        + getFieldName()
        + ", params="
        + getExtraParams()
        + '}';
  }

  IndexParam grpc() {
    return builder.build();
  }

  private Map<String, String> toMap(List<KeyValuePair> extraParams) {
    return extraParams.stream()
        .collect(Collectors.toMap(KeyValuePair::getKey, KeyValuePair::getValue));
  }
}
