package io.milvus.v2.service.vector.request;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SearchReqTest {

    @Test
    public void testSearchReqEmptyTopKWithLimit() {
        SearchReq searchReq=SearchReq.builder()
                .collectionName("test_collection")
                .limit(10)
                .filter("some filter string")
                .build();
        assertEquals(10, searchReq.getTopK());

    }
}