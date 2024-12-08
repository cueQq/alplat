/*
 * Copyright (c) 2024. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package es;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.time.LocalDateTime;

public class ExponentialSmoothingModel {

    /*public static class ExponentialSmoothingFunction extends RichMapFunction<Double, Double> {

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
        @Override
        public Tuple2<LocalDateTime, Double> map(Tuple2<LocalDateTime, Double> value) throws Exception {
            Double last = lastSmoothedValue.value();
            Double smoothedValue;
            if (last == null) {
                // 初始情况下，平滑值等于当前值
                smoothedValue = value.f1;
            } else {
                // 计算新的平滑值
                smoothedValue = alpha * value.f1 + (1 - alpha) * last;
            }
            // 更新状态
            lastSmoothedValue.update(smoothedValue);
            return new Tuple2<>(value.f0, smoothedValue);
        }
    }*/
    public static class ExponentialSmoothingFunction extends RichMapFunction<Tuple2<LocalDateTime, Double>, Tuple2<LocalDateTime, Double>> {

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
        public Tuple2<LocalDateTime, Double> map(Tuple2<LocalDateTime, Double> value) throws Exception {
            Double last = lastSmoothedValue.value();
            Double smoothedValue;
            if (last == null) {
                // 初始情况下，平滑值等于当前值
                smoothedValue = value.f1;
            } else {
                // 计算新的平滑值
                smoothedValue = alpha * value.f1 + (1 - alpha) * last;
            }
            // 更新状态
            lastSmoothedValue.update(smoothedValue);
            return new Tuple2<>(value.f0, smoothedValue);
        }
    }
}
