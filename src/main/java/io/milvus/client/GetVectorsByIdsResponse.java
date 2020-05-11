package io.milvus.client;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Contains the returned <code>response</code> and either a <code>List</code> of <code>floatVectors</code> or <code>
 * binaryVectors</code> for <code>getVectorsByIds</code>. If the id does not exist, both float and binary
 * vectors corresponding to the id will be empty.
 */
public class GetVectorsByIdsResponse {
  private final Response response;
  private final List<List<Float>> floatVectors;
  private final List<ByteBuffer> binaryVectors;

  GetVectorsByIdsResponse(Response response, List<List<Float>> floatVectors, List<ByteBuffer> binaryVectors) {
    this.response = response;
    this.floatVectors = floatVectors;
    this.binaryVectors = binaryVectors;
  }

  public List<List<Float>> getFloatVectors() {
    return floatVectors;
  }

  /**
   * @return a <code>List</code> of <code>ByteBuffer</code> object
   */
  public List<ByteBuffer> getBinaryVectors() {
    return binaryVectors;
  }

  public Response getResponse() {
    return response;
  }

  /** @return <code>true</code> if the response status equals SUCCESS */
  public boolean ok() {
    return response.ok();
  }

}
