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

public class ExponentialSmoothingModel {


    // 自定义的 RichMapFunction，用于指数平滑
    public static class ExponentialSmoothingFunction extends RichMapFunction<ExponentialSmoothing.DataPoint, ExponentialSmoothing.DataPoint> {

        private transient ValueState<Double> lastSmoothedValue;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化状态，用于保存上一次的平滑值
            ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>(
                    "lastSmoothedValue", Double.class);
            lastSmoothedValue = getRuntimeContext().getState(descriptor);
        }

        @Override
        public ExponentialSmoothing.DataPoint map(ExponentialSmoothing.DataPoint dataPoint) throws Exception {
            // 获取上一次的平滑值
            Double last = lastSmoothedValue.value();
            Double smoothedValue;

            // 使用 CSV 中的 alpha 值进行平滑计算
            double alpha = dataPoint.alpha;

            if (last == null) {
                // 初始情况下，平滑值等于当前值
                smoothedValue = dataPoint.value;
            } else {
                // 计算新的平滑值
                smoothedValue = alpha * dataPoint.value + (1 - alpha) * last;
            }
            // 更新状态
            lastSmoothedValue.update(smoothedValue);

            // 返回新的数据点，包含时间戳和经过平滑后的值，不输出 alpha
            return new ExponentialSmoothing.DataPoint(dataPoint.timestamp, smoothedValue, alpha);
        }
    }
}
