package org.example;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ExponentialSmoothing {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置全局并行度为 1(输出就不会出现18>)
        env.setParallelism(1);

        // 模拟数据源，可以替换为实际的数据源
        DataStream<Double> dataStream = env.fromElements(20.0, 21.0, 19.0, 18.0, 20.0);

        // 设置指数平滑的参数 alpha
        double alpha = 0.5;

        // 对数据流应用指数平滑函数
        DataStream<Double> smoothedStream = dataStream
                .keyBy(value -> 0) // 使用 keyBy 保证状态的正确性
                .map(new ExponentialSmoothingFunction(alpha));

        // 打印结果
        smoothedStream.print();

        // 执行任务
        env.execute("Exponential Smoothing Job");
    }

    // 自定义的 RichMapFunction，用于指数平滑
    public static class ExponentialSmoothingFunction extends RichMapFunction<Double, Double> {

        private final double alpha;
        private transient ValueState<Double> lastSmoothedValue;

        public ExponentialSmoothingFunction(double alpha) {
            this.alpha = alpha;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化状态，用于保存上一次的平滑值
            ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>(
                    "lastSmoothedValue", Double.class);
            lastSmoothedValue = getRuntimeContext().getState(descriptor);
        }

        @Override
        public Double map(Double value) throws Exception {
            Double last = lastSmoothedValue.value();
            Double smoothedValue;
            if (last == null) {
                // 初始情况下，平滑值等于当前值
                smoothedValue = value;
            } else {
                // 计算新的平滑值
                smoothedValue = alpha * value + (1 - alpha) * last;
            }
            // 更新状态
            lastSmoothedValue.update(smoothedValue);
            return smoothedValue;
        }
    }
}