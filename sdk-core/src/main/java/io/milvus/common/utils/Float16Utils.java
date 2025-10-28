package io.milvus.common.utils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.util.ArrayList;
import java.util.List;

public class Float16Utils {
    /**
     * Converts a float32 into bf16. May not produce correct values for subnormal floats.
     * <p>
     * This method is copied from microsoft ONNX Runtime:
     * https://github.com/microsoft/onnxruntime/blob/main/java/src/main/jvm/ai/onnxruntime/platform/Fp16Conversions.java
     *
     * @param input a standard float32 value which will be converted to a bfloat16 value
     * @return a short value to store the bfloat16 value
     */
    public static short floatToBf16(float input) {
        int bits = Float.floatToIntBits(input);
        int lsb = (bits >> 16) & 1;
        int roundingBias = 0x7fff + lsb;
        bits += roundingBias;
        return (short) (bits >> 16);
    }

    /**
     * Upcasts a bf16 value stored in a short into a float32 value.
     * <p>
     * This method is copied from microsoft ONNX Runtime:
     * https://github.com/microsoft/onnxruntime/blob/main/java/src/main/jvm/ai/onnxruntime/platform/Fp16Conversions.java
     *
     * @param input a bfloat16 value which will be converted to a float32 value
     * @return a float32 value converted from a bfloat16
     */
    public static float bf16ToFloat(short input) {
        int bits = input << 16;
        return Float.intBitsToFloat(bits);
    }

    /**
     * Rounds a float32 value to a fp16 stored in a short.
     * <p>
     * This method is copied from microsoft ONNX Runtime:
     * https://github.com/microsoft/onnxruntime/blob/main/java/src/main/jvm/ai/onnxruntime/platform/Fp16Conversions.java
     *
     * @param input a standard float32 value which will be converted to a float16 value
     * @return a short value to store the float16 value
     */
    public static short floatToFp16(float input) {
        // Port of MLAS_Float2Half from onnxruntime/core/mlas/inc/mlas_float16.h
        int bits = Float.floatToIntBits(input);
        final int F32_INFINITY = Float.floatToIntBits(Float.POSITIVE_INFINITY);
        final int F16_MAX = (127 + 16) << 23;
        final int DENORM_MAGIC = ((127 - 15) + (23 - 10) + 1) << 23;
        final int SIGN_MASK = 0x80000000;
        final int ROUNDING_CONST = ((15 - 127) << 23) + 0xfff;

        int sign = bits & SIGN_MASK;
        // mask out sign bit
        bits ^= sign;

        short output;
        if (bits >= F16_MAX) {
            // Inf or NaN (all exponent bits set)
            output = (bits > F32_INFINITY) ? (short) 0x7e00 : (short) 0x7c00;
        } else {
            if (bits < (113 << 23)) {
                // Subnormal or zero
                // use a magic value to align our 10 mantissa bits at the bottom of
                // the float. as long as FP addition is round-to-nearest-even this
                // just works.
                float tmp = Float.intBitsToFloat(bits) + Float.intBitsToFloat(DENORM_MAGIC);

                // and one integer subtract of the bias later, we have our final float!
                output = (short) (Float.floatToIntBits(tmp) - DENORM_MAGIC);
            } else {
                int mant_odd = (bits >> 13) & 1; // resulting mantissa is odd

                // update exponent, rounding bias part 1
                bits += ROUNDING_CONST;
                // rounding bias part 2
                bits += mant_odd;
                // take the bits!
                output = (short) (bits >> 13);
            }
        }

        // Add the sign back in
        output = (short) (output | ((short) (sign >> 16)));

        return output;
    }

    /**
     * Upcasts a fp16 value stored in a short to a float32 value.
     * <p>
     * This method is copied from microsoft ONNX Runtime:
     * https://github.com/microsoft/onnxruntime/blob/main/java/src/main/jvm/ai/onnxruntime/platform/Fp16Conversions.java
     *
     * @param input a float16 value which will be converted to a float32 value
     * @return a float32 value converted from a float16 value
     */
    public static float fp16ToFloat(short input) {
        // Port of MLAS_Half2Float from onnxruntime/core/mlas/inc/mlas_float16.h
        final int MAGIC = 113 << 23;
        // exponent mask after shift
        final int SHIFTED_EXP = 0x7c00 << 13;

        // exponent/mantissa bits
        int bits = (input & 0x7fff) << 13;
        // just the exponent
        final int exp = SHIFTED_EXP & bits;
        // exponent adjust
        bits += (127 - 15) << 23;

        // handle exponent special cases
        if (exp == SHIFTED_EXP) {
            // Inf/NaN?
            // extra exp adjust
            bits += (128 - 16) << 23;
        } else if (exp == 0) {
            // Zero/Denormal?
            // extra exp adjust
            bits += (1 << 23);
            // renormalize
            float tmp = Float.intBitsToFloat(bits) - Float.intBitsToFloat(MAGIC);
            bits = Float.floatToIntBits(tmp);
        }

        // sign bit
        bits |= (input & 0x8000) << 16;

        return Float.intBitsToFloat(bits);
    }

    /**
     * Rounds a float32 vector to bf16 values, and stores into a ByteBuffer.
     *
     * @param vector a float32 vector
     * @return <code>ByteBuffer</code> the vector is converted to bfloat16 values and stored into a ByteBuffer
     */
    public static ByteBuffer f32VectorToBf16Buffer(List<Float> vector) {
        if (vector.isEmpty()) {
            return null;
        }

        ByteBuffer buf = ByteBuffer.allocate(2 * vector.size());
        buf.order(ByteOrder.LITTLE_ENDIAN); // milvus server stores fp16/bf16 vector as little endian
        for (Float val : vector) {
            short bf16 = floatToBf16(val);
            buf.putShort(bf16);
        }
        return buf;
    }

    /**
     * Converts a ByteBuffer to fp16 vector upcasts to float32 array.
     *
     * @param buf a buffer to store a float16 vector
     * @return List of Float a float32 vector
     */
    public static List<Float> fp16BufferToVector(ByteBuffer buf) {
        buf.rewind(); // reset the read position
        List<Float> vector = new ArrayList<>();
        ShortBuffer sbuf = buf.asShortBuffer();
        for (int i = 0; i < sbuf.limit(); i++) {
            float val = fp16ToFloat(sbuf.get(i));
            vector.add(val);
        }
        return vector;
    }

    /**
     * Rounds a float32 vector to fp16 values, and stores into a ByteBuffer.
     *
     * @param vector a float32 vector
     * @return <code>ByteBuffer</code> the vector is converted to float16 values and stored in a ByteBuffer
     */
    public static ByteBuffer f32VectorToFp16Buffer(List<Float> vector) {
        if (vector.isEmpty()) {
            return null;
        }

        ByteBuffer buf = ByteBuffer.allocate(2 * vector.size());
        buf.order(ByteOrder.LITTLE_ENDIAN); // milvus server stores fp16/bf16 vector as little endian
        for (Float val : vector) {
            short bf16 = floatToFp16(val);
            buf.putShort(bf16);
        }
        return buf;
    }

    /**
     * Converts a ByteBuffer to bf16 vector upcasts to float32 array.
     *
     * @param buf a buffer to store a bfloat16 vector
     * @return List of Float the vector is converted to float32 values
     */
    public static List<Float> bf16BufferToVector(ByteBuffer buf) {
        buf.rewind(); // reset the read position
        List<Float> vector = new ArrayList<>();
        ShortBuffer sbuf = buf.asShortBuffer();
        for (int i = 0; i < sbuf.limit(); i++) {
            float val = bf16ToFloat(sbuf.get(i));
            vector.add(val);
        }
        return vector;
    }

    /**
     * Stores a fp16/bf16 vector into a ByteBuffer.
     *
     * @param vector a float16 vector stored in a list of Short
     * @return <code>ByteBuffer</code> a buffer to store the float16 vector
     */
    public static ByteBuffer f16VectorToBuffer(List<Short> vector) {
        if (vector.isEmpty()) {
            return null;
        }

        ByteBuffer buf = ByteBuffer.allocate(2 * vector.size());
        buf.order(ByteOrder.LITTLE_ENDIAN); // milvus server stores fp16/bf16 vector as little endian
        for (Short val : vector) {
            buf.putShort(val);
        }
        return buf;
    }

    /**
     * Converts a ByteBuffer to a fp16/bf16 vector stored in short array.
     *
     * @param buf a buffer to store a float16 vector
     * @return List of Short the vector is converted to a list of Short, each Short value is a float16 value
     */
    public static List<Short> bufferToF16Vector(ByteBuffer buf) {
        buf.rewind(); // reset the read position
        List<Short> vector = new ArrayList<>();
        ShortBuffer sbuf = buf.asShortBuffer();
        for (int i = 0; i < sbuf.limit(); i++) {
            vector.add(sbuf.get(i));
        }
        return vector;
    }
}
