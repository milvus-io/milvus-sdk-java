package io.milvus.client.dsl;

import io.milvus.client.SearchParam;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class RangeQuery<T> extends Query {
  private Schema.Field<T> field;
  private List<Expr> exprs = new ArrayList<>();

  RangeQuery(Schema.Field field) {
    this.field = field;
  }

  public RangeQuery<T> gt(T value) {
    exprs.add(new Expr(Type.GT, value));
    return this;
  }

  public RangeQuery<T> gte(T value) {
    exprs.add(new Expr(Type.GTE, value));
    return this;
  }

  public RangeQuery<T> lt(T value) {
    exprs.add(new Expr(Type.LT, value));
    return this;
  }

  public RangeQuery<T> lte(T value) {
    exprs.add(new Expr(Type.LTE, value));
    return this;
  }

  @Override
  protected JSONObject buildSearchParam(SearchParam searchParam, JSONObject outer) {
    return outer.put("range", new JSONObject().put(field.name, buildSearchParam(exprs)));
  }

  private JSONObject buildSearchParam(List<Expr> exprs) {
    JSONObject json = new JSONObject();
    exprs.forEach(e -> json.put(e.type.name().toLowerCase(), e.value));
    return json;
  }

  public enum Type {
    GT, GTE, LT, LTE;
  }

  private class Expr {
    Type type;
    T value;

    Expr(Type type, T value) {
      this.type = type;
      this.value = value;
    }
  }
}
