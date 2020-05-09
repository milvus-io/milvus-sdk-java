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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 * Contains the returned <code>response</code> and <code>queryResultsList</code> for <code>search
 * </code>
 */
public class SearchResponse {

  private Response response;
  private int numQueries;
  private long topK;
  private List<List<Long>> resultIdsList;
  private List<List<Float>> resultDistancesList;

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
   *     the query result of a vector.
   */
  public List<List<QueryResult>> getQueryResultsList() {
    return IntStream.range(0, numQueries)
        .mapToObj(
            i ->
                LongStream.range(0, resultIdsList.get(i).size())
                    .mapToObj(
                        j ->
                            new QueryResult(
                                resultIdsList.get(i).get((int) j),
                                resultDistancesList.get(i).get((int) j)))
                    .collect(Collectors.toList()))
        .collect(Collectors.toList());
  }

  /**
   * @return a <code>List</code> of result ids. Each inner <code>List</code> contains the result ids
   *     of a vector.
   */
  public List<List<Long>> getResultIdsList() {
    return resultIdsList;
  }

  void setResultIdsList(List<List<Long>> resultIdsList) {
    this.resultIdsList = resultIdsList;
  }

  /**
   * @return @return a <code>List</code> of result distances. Each inner <code>List</code> contains
   *     the result distances of a vector.
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
   * Represents a single result of a vector query. Contains the result <code>vectorId</code> and its
   * <code>distance</code> to the vector being queried
   */
  public static class QueryResult {
    private final long vectorId;
    private final float distance;

    QueryResult(long vectorId, float distance) {
      this.vectorId = vectorId;
      this.distance = distance;
    }

    public long getVectorId() {
      return vectorId;
    }

    public float getDistance() {
      return distance;
    }
  }
}
