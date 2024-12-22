package es;

import org.apache.flink.ml.common.param.HasFeaturesCol;
import org.apache.flink.ml.common.param.HasPredictionCol;
import org.apache.flink.ml.param.DoubleParam;
import org.apache.flink.ml.param.Param;

public interface ExponentialSmoothingParams<T> extends HasFeaturesCol<T>, HasPredictionCol<T> {

    // 平滑因子
    Param<Double> SMOOTHING_FACTOR =
            new DoubleParam("smoothingFactor", "The smoothing factor for exponential smoothing.", 0.5);

    default Double getSmoothingFactor() {
        return get(SMOOTHING_FACTOR);
    }

    default T setSmoothingFactor(Double value) {
        return set(SMOOTHING_FACTOR, value);
    }
}