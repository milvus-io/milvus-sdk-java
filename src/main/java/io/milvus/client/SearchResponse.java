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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 * Contains the returned <code>response</code> and query results for <code>search</code>
 */
public class SearchResponse {

  private Response response;
  private int numQueries;
  private long topK;
  private List<List<Long>> resultIdsList;
  private List<List<Float>> resultDistancesList;
  private List<Map<String, Object>> fieldsMap;

  public int getNumQueries() {
    return numQueries;
  }

  void setNumQueries(int numQueries) {
    this.numQueries = numQueries;
  }

  public long getTopK() {
    return topK;
  }

  void setTopK(long topK) {
    this.topK = topK;
  }

  /**
   * @return a <code>List</code> of <code>QueryResult</code>s. Each inner <code>List</code> contains
   *     the query result of an entity.
   */
  public List<List<QueryResult>> getQueryResultsList() {
    return IntStream.range(0, numQueries)
        .mapToObj(
            i ->
                IntStream.range(0, resultIdsList.get(i).size())
                    .mapToObj(
                        j ->
                            new QueryResult(
                                resultIdsList.get(i).get(j),
                                resultDistancesList.get(i).get(j)))
                    .collect(Collectors.toList()))
        .collect(Collectors.toList());
  }

  /**
   * @return a <code>List</code> of result ids. Each inner <code>List</code> contains the result ids
   *     of an entity.
   */
  public List<List<Long>> getResultIdsList() {
    return resultIdsList;
  }

  void setResultIdsList(List<List<Long>> resultIdsList) {
    this.resultIdsList = resultIdsList;
  }

  /**
   * @return a <code>List</code> of result distances. Each inner <code>List</code> contains
   *     the result distances of an entity.
   */
  public List<List<Float>> getResultDistancesList() {
    return resultDistancesList;
  }

  void setResultDistancesList(List<List<Float>> resultDistancesList) {
    this.resultDistancesList = resultDistancesList;
  }

  public Response getResponse() {
    return response;
  }

  void setResponse(Response response) {
    this.response = response;
  }

  /**
   * @return A <code>List</code> of map with fields information. The list order corresponds to
   * <code>resultIdsList</code>. Each <code>Map</code> maps field names to records in a row.
   * The record object can be one of int, long, float, double, List<Float> or ByteBuffer
   * depending on the field's <code>DataType</code> you specified.
   */
  public List<Map<String, Object>> getFieldsMap() { return fieldsMap; }

  void setFieldsMap(List<Map<String, Object>> fieldsMap) {
    this.fieldsMap = fieldsMap;
  }

  /** @return <code>true</code> if the response status equals SUCCESS */
  public boolean ok() {
    return response.ok();
  }

  @Override
  public String toString() {
    return String.format(
        "SearchResponse {%s, returned results for %d queries}", response.toString(), numQueries);
  }

  /**
   * Represents a single result of an entity query. Contains the result <code>entityId</code> and its
   * <code>distance</code> to the entity being queried
   */
  public static class QueryResult {
    private final long entityId;
    private final float distance;

    QueryResult(long entityId, float distance) {
      this.entityId = entityId;
      this.distance = distance;
    }

    public long getEntityId() {
      return entityId;
    }

    public float getDistance() {
      return distance;
    }
  }
}
