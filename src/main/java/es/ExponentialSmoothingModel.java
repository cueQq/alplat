package es;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

public class ExponentialSmoothingModel {

    public static class ExponentialSmoothingFunction extends RichMapFunction<Double, Double> {

        private final double alpha;
        private transient ValueState<Double> lastSmoothedValue;

        public ExponentialSmoothingFunction(double alpha) {
            this.alpha = alpha;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // Initialize state to store the last smoothed value
            ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>(
                    "lastSmoothedValue", Double.class);
            lastSmoothedValue = getRuntimeContext().getState(descriptor);
        }

        @Override
        public Double map(Double value) throws Exception {
            Double last = lastSmoothedValue.value();
            Double smoothedValue;
            if (last == null) {
                // Initially, the smoothed value is equal to the current value
                smoothedValue = value;
            } else {
                // Calculate the new smoothed value
                smoothedValue = alpha * value + (1 - alpha) * last;
            }
            // Update the state
            lastSmoothedValue.update(smoothedValue);
            return smoothedValue;
        }
    }
}