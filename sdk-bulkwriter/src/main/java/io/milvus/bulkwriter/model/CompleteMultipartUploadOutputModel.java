package io.milvus.bulkwriter.model;

import org.simpleframework.xml.Element;
import org.simpleframework.xml.Namespace;
import org.simpleframework.xml.Root;

@Root(name = "CompleteMultipartUploadOutput", strict = false)
@Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
public class CompleteMultipartUploadOutputModel {
    @Element(name = "Location")
    private String location;

    @Element(name = "Bucket")
    private String bucket;

    @Element(name = "Key")
    private String object;

    @Element(name = "ETag")
    private String etag;

    public CompleteMultipartUploadOutputModel() {
    }

    public String location() {
        return location;
    }

    public String bucket() {
        return bucket;
    }

    public String object() {
        return object;
    }

    public String etag() {
        return etag;
    }
}
