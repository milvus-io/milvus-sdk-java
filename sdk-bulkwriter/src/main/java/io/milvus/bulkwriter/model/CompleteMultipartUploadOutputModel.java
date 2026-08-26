package io.milvus.bulkwriter.model;

import org.simpleframework.xml.Element;
import org.simpleframework.xml.Namespace;
import org.simpleframework.xml.Root;

/**
 * XML output model for the response of a complete multipart upload request.
 *
 * <p>It is deserialized from the S3-compatible response and carries the location, bucket,
 * object key, and ETag of the completed upload.</p>
 */
@Root(name = "CompleteMultipartUploadOutput", strict = false)
@Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
public class CompleteMultipartUploadOutputModel {
    /**
     * The location (URL) of the uploaded object.
     */
    @Element(name = "Location")
    private String location;

    /**
     * The bucket that contains the uploaded object.
     */
    @Element(name = "Bucket")
    private String bucket;

    /**
     * The key of the uploaded object.
     */
    @Element(name = "Key")
    private String object;

    /**
     * The ETag of the uploaded object.
     */
    @Element(name = "ETag")
    private String etag;

    /**
     * Constructs an empty {@code CompleteMultipartUploadOutputModel}.
     */
    public CompleteMultipartUploadOutputModel() {
    }

    /**
     * Returns the location (URL) of the uploaded object.
     *
     * @return the object location
     */
    public String location() {
        return location;
    }

    /**
     * Returns the bucket that contains the uploaded object.
     *
     * @return the bucket name
     */
    public String bucket() {
        return bucket;
    }

    /**
     * Returns the key of the uploaded object.
     *
     * @return the object key
     */
    public String object() {
        return object;
    }

    /**
     * Returns the ETag of the uploaded object.
     *
     * @return the object ETag
     */
    public String etag() {
        return etag;
    }
}
