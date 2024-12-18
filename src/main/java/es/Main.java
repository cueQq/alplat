package es;

import beifen.ExponentialSmoothing;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Main {

    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 输入数据：假设是一个CSV文件，每行包含时间戳和某个值
        String filePath = "path/to/your/input.csv"; // 输入文件路径
        DataStream<DataPoint> dataStream = env.readTextFile(filePath)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new DataPoint(fields[0], Double.parseDouble(fields[1]));
                });

        // 将 DataStream 转换为 Table
        Table inputTable = tEnv.fromDataStream(dataStream);

        // 设置指数平滑的平滑因子（alpha）
        ExponentialSmoothing es = new ExponentialSmoothing();
        es.setSmoothingFactor(0.3); // 例如设置 alpha = 0.3

        // 应用指数平滑算法
        Table resultTable = es.transform(inputTable)[0]; // 获取处理结果（第一个输出 Table）

        // 输出结果 Table
        tEnv.toDataStream(resultTable).print(); // 将结果打印到控制台

        // 执行 Flink 作业
        env.execute("Exponential Smoothing Job");
    }

    // 数据点类
    public static class DataPoint {
        public String timestamp;
        public double value;

        public DataPoint(String timestamp, double value) {
            this.timestamp = timestamp;
            this.value = value;
        }

        @Override
        public String toString() {
            return timestamp + ": " + value;
        }
    }
}
