package io.milvus;

import io.milvus.common.utils.Float16Utils;
import io.milvus.grpc.DataType;
import org.junit.jupiter.api.Assertions;

import java.nio.ByteBuffer;
import java.util.*;

public class TestUtils {
    private int dimension = 256;
    private static final Random RANDOM = new Random();

    public static final String MilvusDockerImageID = "milvusdb/milvus:2.6-20251015-bb4446e5-amd64";

    public TestUtils(int dimension) {
        this.dimension = dimension;
    }

    public List<Float> generateFloatVector(int dim) {
        List<Float> vector = new ArrayList<>();
        for (int i = 0; i < dim; ++i) {
            vector.add(RANDOM.nextFloat());
        }
        return vector;
    }

    public List<Float> generateFloatVector() {
        return generateFloatVector(dimension);
    }

    public List<List<Float>> generateFloatVectors(int count) {
        List<List<Float>> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            vectors.add(generateFloatVector());
        }

        return vectors;
    }

    public ByteBuffer generateBinaryVector(int dim) {
        int byteCount = dim / 8;
        ByteBuffer vector = ByteBuffer.allocate(byteCount);
        for (int i = 0; i < byteCount; ++i) {
            vector.put((byte) RANDOM.nextInt(Byte.MAX_VALUE));
        }
        return vector;
    }

    public ByteBuffer generateBinaryVector() {
        return generateBinaryVector(dimension);
    }

    public List<ByteBuffer> generateBinaryVectors(int count) {
        List<ByteBuffer> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            vectors.add(generateBinaryVector());
        }
        return vectors;

    }

    public ByteBuffer generateFloat16Vector() {
        List<Float> vector = generateFloatVector();
        return Float16Utils.f32VectorToFp16Buffer(vector);
    }

    public List<ByteBuffer> generateFloat16Vectors(int count) {
        List<ByteBuffer> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            vectors.add(generateFloat16Vector());
        }
        return vectors;
    }

    public ByteBuffer generateBFloat16Vector() {
        List<Float> vector = generateFloatVector();
        return Float16Utils.f32VectorToBf16Buffer(vector);
    }

    public List<ByteBuffer> generateBFloat16Vectors(int count) {
        List<ByteBuffer> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            vectors.add(generateBFloat16Vector());
        }
        return vectors;
    }

    public SortedMap<Long, Float> generateSparseVector() {
        SortedMap<Long, Float> sparse = new TreeMap<>();
        int dim = RANDOM.nextInt(10) + 10;
        while (sparse.size() < dim) {
            sparse.put((long) RANDOM.nextInt(1000000), RANDOM.nextFloat());
        }
        return sparse;
    }

    public List<SortedMap<Long, Float>> generateSparseVectors(int count) {
        List<SortedMap<Long, Float>> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            vectors.add(generateSparseVector());
        }
        return vectors;
    }

    public List<?> generateRandomArray(DataType eleType, int maxCapacity) {
        switch (eleType) {
            case Bool: {
                List<Boolean> values = new ArrayList<>();
                for (int i = 0; i < maxCapacity; i++) {
                    values.add(i%10 == 0);
                }
                return values;
            }
            case Int8:
            case Int16: {
                List<Short> values = new ArrayList<>();
                for (int i = 0; i < maxCapacity; i++) {
                    values.add((short)RANDOM.nextInt(256));
                }
                return values;
            }
            case Int32: {
                List<Integer> values = new ArrayList<>();
                for (int i = 0; i < maxCapacity; i++) {
                    values.add(RANDOM.nextInt());
                }
                return values;
            }
            case Int64: {
                List<Long> values = new ArrayList<>();
                for (int i = 0; i < maxCapacity; i++) {
                    values.add(RANDOM.nextLong());
                }
                return values;
            }
            case Float: {
                List<Float> values = new ArrayList<>();
                for (int i = 0; i < maxCapacity; i++) {
                    values.add(RANDOM.nextFloat());
                }
                return values;
            }
            case Double: {
                List<Double> values = new ArrayList<>();
                for (int i = 0; i < maxCapacity; i++) {
                    values.add(RANDOM.nextDouble());
                }
                return values;
            }
            case VarChar: {
                List<String> values = new ArrayList<>();
                for (int i = 0; i < maxCapacity; i++) {
                    values.add(String.format("varchar_arr_%d", i));
                }
                return values;
            }
            default:
                Assertions.fail();
        }
        return null;
    }
}
