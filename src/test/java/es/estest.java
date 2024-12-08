package es;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class estest {

    public static void main(String[] args) throws Exception {
        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set global parallelism to 1 (output will not be interleaved)
        env.setParallelism(1);

        // Read data from CSV file
        DataStream<String> textStream = env.readTextFile("./es.csv");

        // Print the raw data to verify if it is read correctly
        textStream.print("Raw Data");

        // Convert string data to Double type
        DataStream<Double> dataStream = textStream.map(Double::parseDouble);

        // Set the parameter alpha for exponential smoothing
        double alpha = 0.5;

        // Apply exponential smoothing function to the data stream
        DataStream<Double> smoothedStream = dataStream
                .keyBy(value -> 0) // Use keyBy to ensure state correctness
                .map(new ExponentialSmoothingFunction(alpha));

        // Print the result
        smoothedStream.print("Smoothed Data");

        // Execute the job
        env.execute("Exponential Smoothing Job");
    }

    // Custom RichMapFunction for exponential smoothing
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