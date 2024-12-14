package es;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class ExponentialSmoothing {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置全局并行度为 1(输出就不会出现18>)
        env.setParallelism(1);

        // 读取CSV文件数据
        String filePath = "src/main/java/es/AirPassengers.csv"; // CSV文件路径
        int totalLines = countLines(filePath);

        DataStream<DataPoint> dataStream = env.readTextFile(filePath)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new DataPoint(fields[0], Double.parseDouble(fields[1]));
                });

        // 设置指数平滑的参数 alpha
        double alpha = readAlphaFromCSV("src/main/java/es/alpha.csv");

        // 对数据流应用指数平滑函数
        ExponentialSmoothingModel.ExponentialSmoothingFunction smoothingFunction = new ExponentialSmoothingModel.ExponentialSmoothingFunction(alpha, totalLines);
        DataStream<DataPoint> smoothedStream = dataStream
                .keyBy(value -> 0) // 使用 keyBy 保证状态的正确性
                .map(smoothingFunction)
                .filter(dataPoint -> dataPoint != null); // 使用 filter 过滤掉 null 值

        // 输出平滑结果
        // smoothedStream.print(); // 移除这行
        env.execute("Exponential Smoothing Job");
    }

    // 定义数据点类，用于存储时间戳和数据值
    public static class DataPoint {
        public String timestamp;
        public double value;

        public DataPoint() {}

        public DataPoint(String timestamp, double value) {
            this.timestamp = timestamp;
            this.value = value;
        }

        @Override
        public String toString() {
            return "[" + timestamp + "] " + value;
        }
    }

    // 读取 CSV 文件中的 alpha 参数
    public static double readAlphaFromCSV(String csvFile) {
        double alpha = -1; // 默认值为 -1 表示未读取到有效的 alpha

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String line;
            boolean isFirstLine = true;

            // 读取 CSV 文件中的每一行
            while ((line = br.readLine()) != null) {
                if (isFirstLine) {
                    // 跳过第一行（标题行）
                    isFirstLine = false;
                    continue;
                }
                // 获取 alpha 值
                alpha = Double.parseDouble(line.trim());
                break; // 只读取第一行的 alpha 值
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return alpha;
    }

    // 读取 CSV 文件的行数
    public static int countLines(String csvFile) {
        int lineCount = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            while (br.readLine() != null) {
                lineCount++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lineCount;
    }


}