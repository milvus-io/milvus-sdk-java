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

import io.milvus.param.R;
import org.tensorflow.ndarray.buffer.ByteDataBuffer;
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

    public static ByteBuffer generateBinaryVector(int dimension) {
        Random ran = new Random();
        int byteCount = dimension / 8;
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

    public static ByteBuffer generateFloat16Vector(int dimension, boolean bfloat16) {
        Random ran = new Random();
        int byteCount = dimension*2;
        ByteBuffer vector = ByteBuffer.allocate(byteCount);
        for (int i = 0; i < dimension; ++i) {
            ByteDataBuffer bf;
            if (bfloat16) {
                TFloat16 tt = TFloat16.scalarOf((float)ran.nextInt(dimension));
                bf = tt.asRawTensor().data();
            } else {
                TBfloat16 tt = TBfloat16.scalarOf((float)ran.nextInt(dimension));
                bf = tt.asRawTensor().data();
            }
            vector.put(bf.getByte(0));
            vector.put(bf.getByte(1));
        }
        return vector;
    }

    public static List<ByteBuffer> generateFloat16Vectors(int dimension, int count, boolean bfloat16) {
        List<ByteBuffer> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            ByteBuffer vector = generateFloat16Vector(dimension, bfloat16);
            vectors.add(vector);
        }
        return vectors;
    }

    public static SortedMap<Long, Float> generateSparseVector() {
        Random ran = new Random();
        SortedMap<Long, Float> sparse = new TreeMap<>();
        int dim = ran.nextInt(10) + 1;
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
