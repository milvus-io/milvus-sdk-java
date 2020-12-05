package io.milvus.client.dsl;

import io.milvus.client.SearchParam;
import java.util.Arrays;
import org.json.JSONObject;

public abstract class Query {

  public static BoolQuery bool(Query... subqueries) {
    return new BoolQuery(BoolQuery.Type.BOOL, Arrays.asList(subqueries));
  }

  public static BoolQuery must(Query... subqueries) {
    return new BoolQuery(BoolQuery.Type.MUST, Arrays.asList(subqueries));
  }

  public static BoolQuery must_not(Query... subqueries) {
    return new BoolQuery(BoolQuery.Type.MUST_NOT, Arrays.asList(subqueries));
  }

  public static BoolQuery should(Query... subqueries) {
    return new BoolQuery(BoolQuery.Type.SHOULD, Arrays.asList(subqueries));
  }

  public SearchParam buildSearchParam(String collectionName) {
    SearchParam searchParam = SearchParam.create(collectionName);
    JSONObject json = buildSearchParam(searchParam, new JSONObject());
    searchParam.setDsl(json);
    return searchParam;
  }

  protected abstract JSONObject buildSearchParam(SearchParam searchParam, JSONObject json);
}
