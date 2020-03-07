package io.milvus.client;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

public class GetVectorIdsResponse {
  private final Response response;
  private final List<Long> ids;

  public GetVectorIdsResponse(Response response, List<Long> ids) {
    this.response = response;
    this.ids = ids;
  }

  public List<Long> getIds() {
    return ids;
  }

  public Response getResponse() {
    return response;
  }

  public boolean ok() {
    return response.ok();
  }
}
