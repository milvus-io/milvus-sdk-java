package io.milvus.client.dsl;

import java.util.List;

public class InsertParam {
  private final io.milvus.client.InsertParam insertParam;

  InsertParam(String collectionName) {
    this.insertParam = io.milvus.client.InsertParam.create(collectionName);
  }

  public InsertParam withIds(List<Long> ids) {
    insertParam.setEntityIds(ids);
    return this;
  }

  public <T> InsertParam with(Schema.Field<T> field, List<T> data) {
    insertParam.addField(field.name, field.dataType, data);
    return this;
  }

  public <T> InsertParam with(Schema.VectorField<T> vectorField, List<T> data) {
    insertParam.addVectorField(vectorField.name, vectorField.dataType, data);
    return this;
  }

  io.milvus.client.InsertParam getInsertParam() {
    return insertParam;
  }
}
