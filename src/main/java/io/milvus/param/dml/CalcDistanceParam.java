package io.milvus.param.dml;

import io.milvus.exception.ParamException;
import io.milvus.param.MetricType;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * only support two vectors cal
 */
public class CalcDistanceParam {
    private final List<Float> vector1;
    private final List<Float> vector2;
    private final MetricType metricType;

    public CalcDistanceParam(@Nonnull List<Float> vector1,@Nonnull List<Float> vector2,@Nonnull MetricType metricType){
        if (vector1.size() != vector2.size()){
            throw new ParamException("size is not equal");
        }

        this.vector1 = vector1;
        this.vector2 = vector2;
        this.metricType = metricType;
    }

    public List<Float> getVector1() {
        return vector1;
    }

    public List<Float> getVector2() {
        return vector2;
    }

    public MetricType getMetricType() {
        return metricType;
    }
}
