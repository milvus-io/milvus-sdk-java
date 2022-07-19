package com.zilliz.milvustest.entity;

import lombok.Data;

import java.util.List;

@Data
public class FileBody {
  private String fieldName;
  private FieldType fieldType;
  private List fieldValue;
}
