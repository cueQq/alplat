package es;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

public class Main {
    public static void main(String[] args) throws Exception {
        // 设置 Flink 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1); // 设置并行度为1

        // 数据文件路径（请确保路径正确）
        String filePath = "src/main/java/es/AirPassengers.csv";

        // 读取 CSV 文件数据
        DataStream<Row> dataStream = ExponentialSmoothing.readCsvFile(env, filePath);

        // 设置平滑因子
        double smoothingFactor = 0.5;

        // 执行指数平滑计算
        ExponentialSmoothing exponentialSmoothing = new ExponentialSmoothing();
        DataStream<Row> resultStream = exponentialSmoothing.computeExponentialSmoothing(dataStream, smoothingFactor);

        // 打印每个窗口的指数平滑结果
        resultStream
                .map(row -> row.getField(0).toString())  // 将 Row 转换为字符串，输出计算的指数平滑值
                .print();  // 打印输出

        // 执行 Flink 作业
        env.execute("Exponential Smoothing Example");
    }
}