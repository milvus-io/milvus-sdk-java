package io.milvus.client;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

public class GetVectorByIdResponse {
  private final Response response;
  private final List<Float> floatVector;
  private final ByteBuffer binaryVector;

  public GetVectorByIdResponse(
      Response response, List<Float> floatVector, ByteBuffer binaryVector) {
    this.response = response;
    this.floatVector = floatVector;
    this.binaryVector = binaryVector;
  }

  public List<Float> getFloatVector() {
    return floatVector;
  }

  /**
   * @return an <code>Optional</code> object which may or may not contain a <code>ByteBuffer</code>
   *     object
   * @see Optional
   */
  public Optional<ByteBuffer> getBinaryVector() {
    return Optional.ofNullable(binaryVector);
  }

  /** @return <code>true</code> if the id corresponds to a float vector */
  public boolean isFloatVector() {
    return !floatVector.isEmpty();
  }

  /** @return <code>true</code> if the id corresponds to a binary vector */
  public boolean isBinaryVector() {
    return binaryVector != null && binaryVector.hasRemaining();
  }

  /** @return <code>true</code> if the id's corresponding vector exists */
  public boolean exists() {
    return isFloatVector() || isBinaryVector();
  }

  public Response getResponse() {
    return response;
  }

  public boolean ok() {
    return response.ok();
  }
}
