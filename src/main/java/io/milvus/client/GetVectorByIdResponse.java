package io.milvus.client;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

/**
 * Contains the returned <code>response</code> and either a <code>floatVector</code> or a <code>
 * binaryVector</code> for each vector of <code>getVectorsById</code>. If the id does not exist, both returned
 * vectors will be empty.
 */
public class GetVectorByIdResponse {
  private final Response response;
  private final List<Float> floatVector;
  private final ByteBuffer binaryVector;

  GetVectorByIdResponse(Response response, List<Float> floatVector, ByteBuffer binaryVector) {
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

  /** @return <code>true</code> if the response status equals SUCCESS */
  public boolean ok() {
    return response.ok();
  }
}
