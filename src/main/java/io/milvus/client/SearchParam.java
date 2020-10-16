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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.milvus.client.exception.InvalidDsl;
import io.milvus.grpc.KeyValuePair;
import io.milvus.grpc.VectorParam;
import io.milvus.grpc.VectorRecord;
import io.milvus.grpc.VectorRowRecord;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Contains parameters for <code>search</code> */
public class SearchParam {
  private static final String VECTOR_QUERY_KEY = "vector";
  private static final String VECTOR_QUERY_PLACEHOLDER = "placeholder";

  private io.milvus.grpc.SearchParam.Builder builder;

  public static SearchParam create(String collectionName) {
    return new SearchParam(collectionName);
  }

  private SearchParam(String collectionName) {
    builder = io.milvus.grpc.SearchParam.newBuilder();
    builder.setCollectionName(collectionName);
  }

  public SearchParam setDsl(String dsl) {
    try {
      JSONObject dslJson = new JSONObject(dsl);
      JSONObject vectorQueryParent = locateVectorQuery(dslJson)
          .orElseThrow(() -> new InvalidDsl("A vector query must be specified", dsl));
      JSONObject vectorQueries = vectorQueryParent.getJSONObject(VECTOR_QUERY_KEY);
      vectorQueryParent.put(VECTOR_QUERY_KEY, VECTOR_QUERY_PLACEHOLDER);
      String vectorQueryField = vectorQueries.keys().next();
      JSONObject vectorQuery = vectorQueries.getJSONObject(vectorQueryField);
      String vectorQueryType = vectorQuery.getString("type");
      JSONArray vectorQueryData = vectorQuery.getJSONArray("query");

      VectorRecord vectorRecord;
      switch (vectorQueryType) {
        case "float":
          vectorRecord = toFloatVectorRecord(vectorQueryData);
          break;
        case "binary":
          vectorRecord = toBinaryVectorRecord(vectorQueryData);
          break;
        default:
          throw new InvalidDsl("Unsupported vector type: " + vectorQueryType, dsl);
      }

      JSONObject json = new JSONObject();
      vectorQuery.remove("type");
      vectorQuery.remove("query");
      json.put("placeholder", vectorQueries);
      VectorParam vectorParam = VectorParam.newBuilder()
          .setJson(json.toString())
          .setRowRecord(vectorRecord)
          .build();

      builder.setDsl(dslJson.toString())
          .addAllVectorParam(ImmutableList.of(vectorParam));
      return this;
    } catch (JSONException e) {
      throw new InvalidDsl(e.getMessage(), dsl);
    }
  }

  public SearchParam setPartitionTags(List<String> partitionTags) {
    builder.addAllPartitionTagArray(partitionTags);
    return this;
  }

  public SearchParam setParamsInJson(String paramsInJson) {
    builder.addExtraParams(KeyValuePair.newBuilder()
        .setKey(MilvusClient.extraParamKey)
        .setValue(paramsInJson)
        .build());
    return this;
  }

  io.milvus.grpc.SearchParam grpc() {
    return builder.build();
  }

  private Optional<JSONObject> locateVectorQuery(Object obj) {
    return obj instanceof JSONObject ? locateVectorQuery((JSONObject) obj)
        : obj instanceof JSONArray ? locateVectorQuery((JSONArray) obj)
        : Optional.empty();
  }

  private Optional<JSONObject> locateVectorQuery(JSONArray array) {
    return StreamSupport.stream(array.spliterator(), false)
        .map(this::locateVectorQuery)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .findFirst();
  }

  private Optional<JSONObject> locateVectorQuery(JSONObject obj) {
    if (obj.opt(VECTOR_QUERY_KEY) instanceof JSONObject) {
      return Optional.of(obj);
    }
    return obj.keySet().stream()
        .map(key -> locateVectorQuery(obj.get(key)))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .findFirst();
  }

  private VectorRecord toFloatVectorRecord(JSONArray data) {
    return VectorRecord.newBuilder().addAllRecords(
        StreamSupport.stream(data.spliterator(), false)
            .map(element -> (JSONArray) element)
            .map(array -> {
              int dimension = array.length();
              List<Float> vector = new ArrayList<>(dimension);
              for (int i = 0; i < dimension; i++) {
                vector.add(array.getFloat(i));
              }
              return VectorRowRecord.newBuilder().addAllFloatData(vector).build();
            })
            .collect(Collectors.toList()))
        .build();
  }

  private VectorRecord toBinaryVectorRecord(JSONArray data) {
    return VectorRecord.newBuilder().addAllRecords(
        StreamSupport.stream(data.spliterator(), false)
            .map(element -> (JSONArray) element)
            .map(array -> {
              int dimension = array.length();
              ByteBuffer bytes = ByteBuffer.allocate(dimension);
              for (int i = 0; i < dimension; i++) {
                bytes.put(array.getNumber(i).byteValue());
              }
              bytes.flip();
              ByteString vector = UnsafeByteOperations.unsafeWrap(bytes);
              return VectorRowRecord.newBuilder().setBinaryData(vector).build();
            })
            .collect(Collectors.toList()))
        .build();
  }
}
