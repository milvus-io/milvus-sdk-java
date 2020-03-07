package io.milvus.client;

import java.util.Optional;

/**
 * Contains the returned <code>response</code> and <code>collectionInfo</code> for <code>
 * showCollectionInfo</code>
 */
public class ShowCollectionInfoResponse {

  private final Response response;
  private final CollectionInfo collectionInfo;

  public ShowCollectionInfoResponse(Response response, CollectionInfo collectionInfo) {
    this.response = response;
    this.collectionInfo = collectionInfo;
  }

  public Response getResponse() {
    return response;
  }

  /**
   * @return an <code>Optional</code> object which may or may not contain an <code>CollectionInfo
   *     </code> object
   * @see Optional
   */
  public Optional<CollectionInfo> getCollectionInfo() {
    return Optional.ofNullable(collectionInfo);
  }

  public boolean ok() {
    return response.ok();
  }
}
