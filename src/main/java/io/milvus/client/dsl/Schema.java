package io.milvus.client.dsl;

import io.milvus.client.CollectionMapping;
import io.milvus.client.DataType;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** An abstract class that allows you to predefine a schema to be used. */
public abstract class Schema {
  private final Map<String, Field> fields = new LinkedHashMap<>();

  Field<?> getField(String name) {
    return fields.get(name);
  }

  CollectionMapping mapToCollection(String collectionName) {
    CollectionMapping mapping = CollectionMapping.create(collectionName);
    fields
        .values()
        .forEach(
            f -> {
              if (f instanceof ScalarField) {
                mapping.addField(f.name, f.dataType);
              } else if (f instanceof VectorField) {
                mapping.addVectorField(f.name, f.dataType, ((VectorField<?>) f).dimension);
              }
            });
    return mapping;
  }

  InsertParam insertInto(String collectionName) {
    return new InsertParam(collectionName);
  }

  public class Field<T> {
    public final String name;
    public final DataType dataType;

    private Field(String name, DataType dataType) {
      this.name = name;
      this.dataType = dataType;
      if (fields.putIfAbsent(name, this) != null) {
        throw new IllegalArgumentException("Field name conflict: " + name);
      }
    }
  }

  public class ScalarField<T> extends Field<T> {
    private ScalarField(String name, DataType dataType) {
      super(name, dataType);
    }

    public RangeQuery<T> gt(T value) {
      return new RangeQuery<T>(this).gt(value);
    }

    public RangeQuery<T> gte(T value) {
      return new RangeQuery<T>(this).gte(value);
    }

    public RangeQuery<T> lt(T value) {
      return new RangeQuery<T>(this).lt(value);
    }

    public RangeQuery<T> lte(T value) {
      return new RangeQuery<T>(this).lte(value);
    }

    @SuppressWarnings("unchecked")
    public TermQuery<T> in(T... values) {
      return new TermQuery<>(this, TermQuery.Type.IN, Arrays.asList(values));
    }
  }

  public class Int32Field extends ScalarField<Integer> {
    public Int32Field(String name) {
      super(name, DataType.INT32);
    }
  }

  public class Int64Field extends ScalarField<Long> {
    public Int64Field(String name) {
      super(name, DataType.INT64);
    }
  }

  public class FloatField extends ScalarField<Float> {
    public FloatField(String name) {
      super(name, DataType.FLOAT);
    }
  }

  public class DoubleField extends ScalarField<Double> {
    public DoubleField(String name) {
      super(name, DataType.DOUBLE);
    }
  }

  public class VectorField<T> extends Field<T> {
    public final int dimension;

    private VectorField(String name, DataType dataType, int dimension) {
      super(name, dataType);
      this.dimension = dimension;
    }

    public VectorQuery<T> query(List<T> queries) {
      return new VectorQuery<>(this, queries);
    }
  }

  public class FloatVectorField extends VectorField<List<Float>> {
    public FloatVectorField(String name, int dimension) {
      super(name, DataType.VECTOR_FLOAT, dimension);
    }
  }

  public class BinaryVectorField extends VectorField<ByteBuffer> {
    public BinaryVectorField(String name, int dimension) {
      super(name, DataType.VECTOR_BINARY, dimension);
    }
  }

  public class Entity {
    private final Map<String, Object> properties;

    Entity(Map<String, Object> properties) {
      this.properties = properties;
    }

    @SuppressWarnings("unchecked")
    public <T> T get(Field<T> field) {
      return (T) properties.get(field.name);
    }
  }
}
