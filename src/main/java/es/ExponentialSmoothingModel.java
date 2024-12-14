package es;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

public class ExponentialSmoothingModel {

    // 计算指数平滑值的函数
    public static class ExponentialSmoothingFunction extends RichMapFunction<ExponentialSmoothing.DataPoint, ExponentialSmoothing.DataPoint> {

        private final double alpha;
        private transient ValueState<Double> lastSmoothedValueState;
        private transient ValueState<Integer> processedCountState;  // 添加一个状态保存处理的数据量
        private final int totalLines; // CSV文件的总行数
        private transient ExponentialSmoothing.DataPoint lastDataPoint;

        public ExponentialSmoothingFunction(double alpha, int totalLines) {
            this.alpha = alpha;
            this.totalLines = totalLines;
        }


        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            // 初始化状态
            ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>(
                    "lastSmoothedValue", // 状态名称
                    Double.class, // 状态类型
                    0.0 // 默认值
            );
            lastSmoothedValueState = getRuntimeContext().getState(descriptor);

            //初始化处理元素的数量
            ValueStateDescriptor<Integer> countDescriptor = new ValueStateDescriptor<>(
                    "processedCount", // 状态名称
                    Integer.class, // 状态类型
                    0 // 默认值
            );
            processedCountState = getRuntimeContext().getState(countDescriptor);
        }

        @Override
        public ExponentialSmoothing.DataPoint map(ExponentialSmoothing.DataPoint value) throws Exception {
            // 获取上一个平滑值
            Double lastSmoothedValue = lastSmoothedValueState.value();

            // 如果是第一次处理，使用当前值作为平滑值
            if (lastSmoothedValue == null) {
                lastSmoothedValue = value.value;
            } else {
                // 使用指数平滑公式更新平滑值
                lastSmoothedValue = alpha * value.value + (1 - alpha) * lastSmoothedValue;
            }

            // 更新状态
            lastSmoothedValueState.update(lastSmoothedValue);


            // 获取当前处理元素的数量
            int currentCount = processedCountState.value();
            //更新状态
            processedCountState.update(currentCount + 1);
            //记录最后一个数据
            lastDataPoint = new ExponentialSmoothing.DataPoint(value.timestamp, lastSmoothedValue);


            // 如果是最后一个元素，则进行预测并输出平滑值
            if (currentCount + 1 == totalLines) {
                printPredictions(lastSmoothedValue, lastDataPoint);
                return new ExponentialSmoothing.DataPoint(value.timestamp, lastSmoothedValue);
            }
            // 返回null值
            return null;
        }


        // 预测并打印未来 5 期的值
        private void printPredictions(double lastSmoothedValue, ExponentialSmoothing.DataPoint lastDataPoint) {
            int predictionSteps = 5; // 预测未来 5 期
            double[] predictions = new double[predictionSteps];
            double predictedValue = lastSmoothedValue;

            System.out.println("Last Smoothed Value: " + lastDataPoint);
            System.out.println("预测值:");
            for (int i = 0; i < predictionSteps; i++) {
                predictedValue = alpha * predictedValue + (1 - alpha) * predictedValue; // 指数平滑公式
                predictions[i] = predictedValue;
                System.out.println("期 " + (i + 1) + ": " + predictedValue);
            }
        }
    }

}