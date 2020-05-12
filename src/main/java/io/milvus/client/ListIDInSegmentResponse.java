package io.milvus.client;

import java.util.List;

/**
 * Contains the returned <code>response</code> and a <code>List</code> of ids present in a segment
 * for <code>listIDInSegment</code>.
 */
public class ListIDInSegmentResponse {
  private final Response response;
  private final List<Long> ids;

  ListIDInSegmentResponse(Response response, List<Long> ids) {
    this.response = response;
    this.ids = ids;
  }

  public List<Long> getIds() {
    return ids;
  }

  public Response getResponse() {
    return response;
  }

  /** @return <code>true</code> if the response status equals SUCCESS */
  public boolean ok() {
    return response.ok();
  }
}
