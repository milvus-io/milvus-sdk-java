package io.milvus.client.dsl;

import io.milvus.client.SearchParam;
import java.util.Collection;
import org.json.JSONArray;
import org.json.JSONObject;

/** Term Query */
public class TermQuery<T> extends Query {
  private final Schema.Field<T> field;
  private final Type type;
  private final Object param;

  public TermQuery(Schema.Field<T> field, Type type, Object param) {
    this.field = field;
    this.type = type;
    this.param = param;
  }

  @Override
  protected JSONObject buildSearchParam(SearchParam searchParam, JSONObject outer) {
    return outer.put("term", new JSONObject().put(field.name, type.toJson(param)));
  }

  /**
   * Term query types.
   * IN: field value should belong to one of the following values
   */
  enum Type {
    IN {
      @Override
      Object toJson(Object param) {
        return new JSONArray((Collection<?>) param);
      }
    };

    abstract Object toJson(Object param);
  }
}
