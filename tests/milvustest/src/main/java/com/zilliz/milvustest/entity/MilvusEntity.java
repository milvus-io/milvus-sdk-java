package com.zilliz.milvustest.entity;

import lombok.Data;

@Data
public class MilvusEntity {
  private String Collection;
  private String partition;
  private String alias;
  private String index;
}
