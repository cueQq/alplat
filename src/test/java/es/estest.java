package es;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class estest {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置全局并行度为 1(输出就不会出现18>)
        env.setParallelism(1);

        // 从CSV文件读取数据流
        String filePath = "src/main/java/es/es.csv"; // 这里替换为你的 CSV 文件路径
        DataStream<String> csvStream = env.readTextFile(filePath);

        // 将 CSV 数据流转换为 DataPoint 数据流，包含 timestamp、value 和 alpha
        DataStream<DataPoint> dataStream = csvStream.map(line -> {
            String[] parts = line.split(",");  // 假设 CSV 格式是: timestamp,value,alpha
            long timestamp = Long.parseLong(parts[0]);
            double value = Double.parseDouble(parts[1]);
            double alpha = Double.parseDouble(parts[2]);
            return new DataPoint(timestamp, value, alpha);
        });

        // 对数据流应用指数平滑函数
        DataStream<DataPoint> smoothedStream = dataStream
                .keyBy(value -> 0) // 使用 keyBy 保证状态的正确性
                .map(new ExponentialSmoothingFunction());

        // 打印结果
        smoothedStream.print();

        // 执行任务
        env.execute("Exponential Smoothing Job");
    }

    // 定义数据点类，用于存储时间戳、数据值和 alpha
    public static class DataPoint {
        public long timestamp;
        public double value;
        public double alpha;

        public DataPoint(long timestamp, double value, double alpha) {
            this.timestamp = timestamp;
            this.value = value;
            this.alpha = alpha;
        }

        @Override
        public String toString() {
            // 不输出 alpha，仅输出时间戳和经过平滑后的值
            return "[" + timestamp + "] " + value;
        }
    }

    // 自定义的 RichMapFunction，用于指数平滑
    public static class ExponentialSmoothingFunction extends RichMapFunction<DataPoint, DataPoint> {

        private transient ValueState<Double> lastSmoothedValue;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化状态，用于保存上一次的平滑值
            ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>(
                    "lastSmoothedValue", Double.class);
            lastSmoothedValue = getRuntimeContext().getState(descriptor);
        }

        @Override
        public DataPoint map(DataPoint dataPoint) throws Exception {
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
            return new DataPoint(dataPoint.timestamp, smoothedValue, alpha);
        }
    }
}
