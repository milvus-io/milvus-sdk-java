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

package io.milvus.bulkwriter.common.utils;

import org.apache.commons.lang3.RandomUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Utility class that generates sample values for the various Milvus field data types, used when
 * producing bulk data files for testing or examples.
 */
public class GeneratorUtils {

    /**
     * Generates a list of {@code count} sequential {@code long} values starting from zero.
     *
     * @param count the number of values to generate
     * @return the generated {@code long} values
     */
    public static List<Long> generatorLongValue(int count) {
        List<Long> result = new ArrayList<>();
        for (int i = 0; i < count; ++i) {
            result.add((long) i);
        }
        return result;
    }

    /**
     * Generates a list of {@code count} {@code boolean} values where every fifth value is {@code true}.
     *
     * @param count the number of values to generate
     * @return the generated {@code boolean} values
     */
    public static List<Boolean> generatorBoolValue(int count) {
        List<Boolean> result = new ArrayList<>();
        for (int i = 0; i < count; ++i) {
            result.add(i % 5 == 0);
        }
        return result;
    }

    /**
     * Generates a list of {@code count} {@code int} values within the Int8 range (0 to 127).
     *
     * @param count the number of values to generate
     * @return the generated Int8 values
     */
    public static List<Integer> generatorInt8Value(int count) {
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < count; ++i) {
            result.add(i % 128);
        }
        return result;
    }

    /**
     * Generates a list of {@code count} {@code int} values within the Int16 range (0 to 999).
     *
     * @param count the number of values to generate
     * @return the generated Int16 values
     */
    public static List<Integer> generatorInt16Value(int count) {
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < count; ++i) {
            result.add(i % 1000);
        }
        return result;
    }

    /**
     * Generates a list of {@code count} {@code int} values within the Int32 range (0 to 99999).
     *
     * @param count the number of values to generate
     * @return the generated Int32 values
     */
    public static List<Integer> generatorInt32Value(int count) {
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < count; ++i) {
            result.add(i % 100000);
        }
        return result;
    }

    /**
     * Generates a list of {@code count} {@code float} values computed as {@code i / 3}.
     *
     * @param count the number of values to generate
     * @return the generated {@code float} values
     */
    public static List<Float> generatorFloatValue(int count) {
        List<Float> result = new ArrayList<>();
        for (int i = 0; i < count; ++i) {
            result.add((float) i / 3);
        }
        return result;
    }

    /**
     * Generates a list of {@code count} {@code double} values computed as {@code i / 7}.
     *
     * @param count the number of values to generate
     * @return the generated {@code double} values
     */
    public static List<Double> generatorDoubleValue(int count) {
        List<Double> result = new ArrayList<>();
        for (int i = 0; i < count; ++i) {
            result.add((double) i / 7);
        }
        return result;
    }

    /**
     * Generates a list of {@code count} random UUID-based strings, each truncated to {@code maxLength}
     * characters, for VarChar fields.
     *
     * @param count     the number of values to generate
     * @param maxLength the maximum length of each generated string
     * @return the generated VarChar values
     */
    public static List<String> generatorVarcharValue(int count, int maxLength) {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < count; ++i) {
            result.add(UUID.randomUUID().toString().substring(0, maxLength));
        }
        return result;
    }

    /**
     * Generates a random binary vector of the given dimension packed into a {@link ByteBuffer}.
     *
     * @param dim the dimension of the binary vector
     * @return the packed binary vector
     */
    public static ByteBuffer generatorBinaryVector(int dim) {
        int[] rawVector = generateRandomBinaryVector(dim);
        return packBits(rawVector);
    }

    private static int[] generateRandomBinaryVector(int dim) {
        int[] rawVector = new int[dim];
        Random random = new Random();

        for (int i = 0; i < dim; i++) {
            rawVector[i] = random.nextInt(2); // random 0 or 1
        }

        return rawVector;
    }

    private static ByteBuffer packBits(int[] rawVector) {
        int byteCount = (int) Math.ceil((double) rawVector.length / 8);
        byte[] binaryArray = new byte[byteCount];

        for (int i = 0; i < rawVector.length; i++) {
            if (rawVector[i] != 0) {
                int byteIndex = i / 8;
                int bitIndex = 7 - (i % 8);
                binaryArray[byteIndex] |= (1 << bitIndex);
            }
        }

        // binary vector doesn't care endian since each byte is independent
        ByteBuffer byteBuffer = ByteBuffer.allocate(byteCount);
        for (byte b : binaryArray) {
            byteBuffer.put(b);
        }
        return byteBuffer;
    }

    /**
     * Generates a list of {@code count} float vectors, each of the given dimension, with values
     * computed as {@code j / 3}.
     *
     * @param dim   the dimension of each float vector
     * @param count the number of float vectors to generate
     * @return the generated float vectors
     */
    public static List<List<Float>> generatorFloatVector(int dim, int count) {
        List<List<Float>> floatVector = new ArrayList<>();

        for (int i = 0; i < count; ++i) {
            List<Float> result = new ArrayList<>();
            for (int j = 0; j < dim; ++j) {
                result.add((float) j / 3);
            }
            floatVector.add(result);
        }
        return floatVector;
    }

    /**
     * Generates a single float vector of the given dimension with random values between 100 and 10000.
     *
     * @param dim the dimension of the float vector
     * @return the generated float vector
     */
    public static List<Float> genFloatVector(int dim) {
        List<Float> result = new ArrayList<>();
        for (int i = 0; i < dim; ++i) {
            result.add(RandomUtils.nextFloat(100, 10000));
        }
        return result;
    }
}
