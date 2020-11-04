package io.milvus.client;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** A class that contains information from Search */
public class SearchResult {
  private final int numQueries;
  private final long topK;
  private final List<List<Long>> resultIdsList;
  private final List<List<Float>> resultDistancesList;
  private final List<List<Map<String, Object>>> fieldsMap;

  public SearchResult(
      int numQueries,
      long topK,
      List<List<Long>> resultIdsList,
      List<List<Float>> resultDistancesList,
      List<List<Map<String, Object>>> fieldsMap) {
    this.numQueries = numQueries;
    this.topK = topK;
    this.resultIdsList = resultIdsList;
    this.resultDistancesList = resultDistancesList;
    this.fieldsMap = fieldsMap;
  }

  public int getNumQueries() {
    return numQueries;
  }

  public long getTopK() {
    return topK;
  }

  public List<List<Long>> getResultIdsList() {
    return resultIdsList;
  }

  public List<List<Float>> getResultDistancesList() {
    return resultDistancesList;
  }

  public List<List<Map<String, Object>>> getFieldsMap() {
    return fieldsMap;
  }

  public List<List<QueryResult>> getQueryResultsList() {
    return IntStream.range(0, numQueries)
        .mapToObj(
            i ->
                IntStream.range(0, resultIdsList.get(i).size())
                    .mapToObj(
                        j ->
                            new QueryResult(
                                resultIdsList.get(i).get(j), resultDistancesList.get(i).get(j)))
                    .collect(Collectors.toList()))
        .collect(Collectors.toList());
  }

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
