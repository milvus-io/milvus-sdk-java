package io.milvus.v2.client;

import io.milvus.v2.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MilvusClientV2Test extends BaseTest {

    @Test
    void testMilvusClientV2() {
    }
    @Test
    void testUseDatabase() {
        try {
            client_v2.useDatabase("test");
        }catch (Exception e) {
            Assertions.assertEquals("Database test not exist", e.getMessage());
        }

    }

}
