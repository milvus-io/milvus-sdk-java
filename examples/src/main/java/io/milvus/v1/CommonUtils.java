/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.milvus.v1;

import io.milvus.common.utils.Float16Utils;
import io.milvus.param.R;

import org.tensorflow.Tensor;
import org.tensorflow.ndarray.Shape;
import org.tensorflow.ndarray.buffer.ByteDataBuffer;
import org.tensorflow.ndarray.buffer.DataBuffers;
import org.tensorflow.types.TBfloat16;
import org.tensorflow.types.TFloat16;

import java.nio.ByteBuffer;
import java.util.*;


public class CommonUtils {

    public static void handleResponseStatus(R<?> r) {
        if (r.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException(r.getMessage());
        }
    }

    public static List<Float> generateFloatVector(int dimension) {
        Random ran = new Random();
        List<Float> vector = new ArrayList<>();
        for (int i = 0; i < dimension; ++i) {
            vector.add(ran.nextFloat());
        }
        return vector;
    }

    public static List<Float> generateFloatVector(int dimension, Float initValue) {
        List<Float> vector = new ArrayList<>();
        for (int i = 0; i < dimension; ++i) {
            vector.add(initValue);
        }
        return vector;
    }

    public static List<List<Float>> generateFloatVectors(int dimension, int count) {
        List<List<Float>> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            List<Float> vector = generateFloatVector(dimension);
            vectors.add(vector);
        }
        return vectors;
    }

    public static List<List<Float>> generateFixFloatVectors(int dimension, int count) {
        List<List<Float>> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            List<Float> vector = generateFloatVector(dimension, (float)n);
            vectors.add(vector);
        }
        return vectors;
    }

    public static void compareFloatVectors(List<Float> vec1, List<Float> vec2) {
        if (vec1.size() != vec2.size()) {
            throw new RuntimeException(String.format("Vector dimension mismatch: %d vs %d", vec1.size(), vec2.size()));
        }
        for (int i = 0; i < vec1.size(); i++) {
            if (Math.abs(vec1.get(i) - vec2.get(i)) > 0.001f) {
                throw new RuntimeException(String.format("Vector value mismatch: %f vs %f at No.%d value",
                        vec1.get(i), vec2.get(i), i));
            }
        }
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    public static ByteBuffer generateBinaryVector(int dimension) {
        Random ran = new Random();
        int byteCount = dimension / 8;
        // binary vector doesn't care endian since each byte is independent
        ByteBuffer vector = ByteBuffer.allocate(byteCount);
        for (int i = 0; i < byteCount; ++i) {
            vector.put((byte) ran.nextInt(Byte.MAX_VALUE));
        }
        return vector;
    }

    public static List<ByteBuffer> generateBinaryVectors(int dimension, int count) {
        List<ByteBuffer> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            ByteBuffer vector = generateBinaryVector(dimension);
            vectors.add(vector);
        }
        return vectors;
    }

    public static void printBinaryVector(ByteBuffer vector) {
        vector.rewind();
        while (vector.hasRemaining()) {
            String byteStr = String.format("%8s", Integer.toBinaryString(vector.get())).replace(' ', '0');
            System.out.print(byteStr);
        }
        System.out.println();
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    public static TBfloat16 genTensorflowBF16Vector(int dimension) {
        Random ran = new Random();
        float[] array = new float[dimension];
        for (int n = 0; n < dimension; ++n) {
            array[n] = ran.nextFloat();
        }

        return TBfloat16.vectorOf(array);
    }

    public static List<TBfloat16> genTensorflowBF16Vectors(int dimension, int count) {
        List<TBfloat16> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
           TBfloat16 vector = genTensorflowBF16Vector(dimension);
            vectors.add(vector);
        }

        return vectors;
    }

    public static ByteBuffer encodeTensorBF16Vector(TBfloat16 vector) {
        ByteDataBuffer tensorBuf = vector.asRawTensor().data();
        ByteBuffer buf = ByteBuffer.allocate((int)tensorBuf.size());
        for (long i = 0; i < tensorBuf.size(); i++) {
            buf.put(tensorBuf.getByte(i));
        }
        return buf;
    }

    public static List<ByteBuffer> encodeTensorBF16Vectors(List<TBfloat16> vectors) {
        List<ByteBuffer> buffers = new ArrayList<>();
        for (TBfloat16 tf : vectors) {
            ByteBuffer bf = encodeTensorBF16Vector(tf);
            buffers.add(bf);
        }
        return buffers;
    }

    public static TBfloat16 decodeBF16VectorToTensor(ByteBuffer buf) {
        if (buf.limit()%2 != 0) {
            return null;
        }
        int dim = buf.limit()/2;
        ByteDataBuffer bf = DataBuffers.of(buf.array());
        return Tensor.of(TBfloat16.class, Shape.of(dim), bf);
    }

    public static List<Float> decodeBF16VectorToFloat(ByteBuffer buf) {
        List<Float> vector = new ArrayList<>();
        TBfloat16 tf = decodeBF16VectorToTensor(buf);
        for (long i = 0; i < tf.size(); i++) {
            vector.add(tf.getFloat(i));
        }
        return vector;
    }


    public static TFloat16 genTensorflowFP16Vector(int dimension) {
        Random ran = new Random();
        float[] array = new float[dimension];
        for (int n = 0; n < dimension; ++n) {
            array[n] = ran.nextFloat();
        }

        return TFloat16.vectorOf(array);
    }

    public static List<TFloat16> genTensorflowFP16Vectors(int dimension, int count) {
        List<TFloat16> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            TFloat16 vector = genTensorflowFP16Vector(dimension);
            vectors.add(vector);
        }

        return vectors;
    }

    public static ByteBuffer encodeTensorFP16Vector(TFloat16 vector) {
        ByteDataBuffer tensorBuf = vector.asRawTensor().data();
        ByteBuffer buf = ByteBuffer.allocate((int)tensorBuf.size());
        for (long i = 0; i < tensorBuf.size(); i++) {
            buf.put(tensorBuf.getByte(i));
        }
        return buf;
    }

    public static List<ByteBuffer> encodeTensorFP16Vectors(List<TFloat16> vectors) {
        List<ByteBuffer> buffers = new ArrayList<>();
        for (TFloat16 tf : vectors) {
            ByteBuffer bf = encodeTensorFP16Vector(tf);
            buffers.add(bf);
        }
        return buffers;
    }

    public static TFloat16 decodeFP16VectorToTensor(ByteBuffer buf) {
        if (buf.limit()%2 != 0) {
            return null;
        }
        int dim = buf.limit()/2;
        ByteDataBuffer bf = DataBuffers.of(buf.array());
        return Tensor.of(TFloat16.class, Shape.of(dim), bf);
    }

    public static List<Float> decodeFP16VectorToFloat(ByteBuffer buf) {
        List<Float> vector = new ArrayList<>();
        TFloat16 tf = decodeFP16VectorToTensor(buf);
        for (long i = 0; i < tf.size(); i++) {
            vector.add(tf.getFloat(i));
        }
        return vector;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    public static ByteBuffer encodeFloat16Vector(List<Float> originVector, boolean bfloat16) {
        if (bfloat16) {
            return Float16Utils.f32VectorToBf16Buffer(originVector);
        } else {
            return Float16Utils.f32VectorToFp16Buffer(originVector);
        }
    }

    public static List<Float> decodeFloat16Vector(ByteBuffer buf, boolean bfloat16) {
        if (bfloat16) {
            return Float16Utils.bf16BufferToVector(buf);
        } else {
            return Float16Utils.fp16BufferToVector(buf);
        }
    }

    public static List<ByteBuffer> encodeFloat16Vectors(List<List<Float>> originVectors, boolean bfloat16) {
        List<ByteBuffer> vectors = new ArrayList<>();
        for (List<Float> originVector : originVectors) {
            if (bfloat16) {
                vectors.add(Float16Utils.f32VectorToBf16Buffer(originVector));
            } else {
                vectors.add(Float16Utils.f32VectorToFp16Buffer(originVector));
            }
        }
        return vectors;
    }

    public static ByteBuffer generateFloat16Vector(int dimension, boolean bfloat16) {
        List<Float> originalVector = generateFloatVector(dimension);
        return encodeFloat16Vector(originalVector, bfloat16);
    }

    public static List<ByteBuffer> generateFloat16Vectors(int dimension, int count, boolean bfloat16) {
        List<ByteBuffer> vectors = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ByteBuffer buf = generateFloat16Vector(dimension, bfloat16);
            vectors.add((buf));
        }
        return vectors;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    public static ByteBuffer generateInt8Vector(int dimension) {
        Random ran = new Random();
        int byteCount = dimension;
        // binary vector doesn't care endian since each byte is independent
        ByteBuffer vector = ByteBuffer.allocate(byteCount);
        for (int i = 0; i < byteCount; ++i) {
            vector.put((byte) (ran.nextInt(256) - 128));
        }
        return vector;
    }

    public static List<ByteBuffer> generateInt8Vectors(int dimension, int count) {
        List<ByteBuffer> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            ByteBuffer vector = generateInt8Vector(dimension);
            vectors.add(vector);
        }
        return vectors;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    public static SortedMap<Long, Float> generateSparseVector() {
        Random ran = new Random();
        SortedMap<Long, Float> sparse = new TreeMap<>();
        int dim = ran.nextInt(10) + 10;
        for (int i = 0; i < dim; ++i) {
            sparse.put((long)ran.nextInt(1000000), ran.nextFloat());
        }
        return sparse;
    }

    public static List<SortedMap<Long, Float>> generateSparseVectors(int count) {
        List<SortedMap<Long, Float>> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            SortedMap<Long, Float> sparse = generateSparseVector();
            vectors.add(sparse);
        }
        return vectors;
    }
}
