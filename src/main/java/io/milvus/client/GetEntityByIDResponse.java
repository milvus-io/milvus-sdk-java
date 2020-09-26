package io.milvus.client;

import java.util.List;
import java.util.Map;

/**
 * Contains the returned <code>response</code>, valid ids within query and a <code>List</code> of
 * fields map for <code>getEntityByID</code>.
 */
public class GetEntityByIDResponse {
  private final Response response;
  private List<Map<String, Object>> fieldsMap;

  GetEntityByIDResponse(
      Response response, List<Map<String, Object>> fieldsMap) {
    this.response = response;
    this.fieldsMap = fieldsMap;
  }

  /**
   * @return A <code>List</code> of map with fields information. The list order corresponds
   * to query IDs. Each <code>Map</code> maps field names to records in a row.
   * The record object can be one of int, long, float, double, List<Float> or List<Byte>
   * depending on the field's <code>DataType</code> you specified.
   */
  public List<Map<String, Object>> getFieldsMap() { return fieldsMap; }

  public Response getResponse() {
    return response;
  }

  /** @return <code>true</code> if the response status equals SUCCESS */
  public boolean ok() {
    return response.ok();
  }
}
