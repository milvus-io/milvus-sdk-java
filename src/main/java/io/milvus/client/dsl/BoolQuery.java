package io.milvus.client.dsl;

import io.milvus.client.SearchParam;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;
import java.util.stream.Collectors;

public class BoolQuery extends Query {
  private final Type type;
  private final List<Query> subqueries;

  BoolQuery(Type type, List<Query> subqueries) {
    this.type = type;
    this.subqueries = subqueries;
  }

  enum Type {
    MUST, MUST_NOT, SHOULD,

    BOOL {
      @Override
      public Object buildSearchParam(SearchParam searchParam, List<Query> subqueries) {
        JSONObject outer = new JSONObject();
        subqueries.forEach(query -> query.buildSearchParam(searchParam, outer));
        return outer;
      }
    };

    public Object buildSearchParam(SearchParam searchParam, List<Query> subqueries) {
      return new JSONArray(subqueries.stream()
          .map(query -> query.buildSearchParam(searchParam, new JSONObject()))
          .collect(Collectors.toList()));
    }
  }

  @Override
  protected JSONObject buildSearchParam(SearchParam searchParam, JSONObject outer) {
    return outer.put(type.name().toLowerCase(), type.buildSearchParam(searchParam, subqueries));
  }
}
