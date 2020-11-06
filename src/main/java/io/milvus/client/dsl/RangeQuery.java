package io.milvus.client.dsl;

import io.milvus.client.SearchParam;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONObject;

/** Range Query */
public class RangeQuery<T> extends Query {
  private final Schema.Field<T> field;
  private final List<Expr> exprs = new ArrayList<>();

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

  /**
   * Range query types.
   * GT: greater than
   * GTE: greater than or equal to
   * LT: less than
   * LTE: less than or equal to
   */
  public enum Type {
    GT,
    GTE,
    LT,
    LTE
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
