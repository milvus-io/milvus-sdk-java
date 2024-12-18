package io.milvus.v2.service.collection.request;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.Map;

@Data
@SuperBuilder
@Deprecated
public class AlterCollectionReq {
    private String collectionName;
    private String databaseName;
    @Builder.Default
    private final Map<String, String> properties = new HashMap<>();



    public static abstract class AlterCollectionReqBuilder<C extends AlterCollectionReq, B extends AlterCollectionReqBuilder<C, B>> {
        public B property(String key, String value) {
            if(null == this.properties$value ){
                this.properties$value = new HashMap<>();
            }
            this.properties$value.put(key, value);
            this.properties$set = true;
            return self();
        }
    }
}
