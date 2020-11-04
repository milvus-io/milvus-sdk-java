package io.milvus.client.dsl;

public class TestBinarySchema extends Schema {
  public final Int32Field intField = new Int32Field("int32");
  public final Int64Field longField = new Int64Field("int64");
  public final FloatField floatField = new FloatField("float");
  public final DoubleField doubleField = new DoubleField("double");
  public final BinaryVectorField binaryVectorField = new BinaryVectorField("binary_vec", 64);
}
