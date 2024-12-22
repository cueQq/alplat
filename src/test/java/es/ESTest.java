package es;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

public class ESTest {

    private StreamExecutionEnvironment env;

    @Before
    public void setUp() {
        // 创建 StreamExecutionEnvironment
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
    }
    @Test
    public void testExponentialSmoothing() throws Exception {
        // 数据文件路径（请确保路径正确）
        String filePath = "src/main/java/es/AirPassengers.csv";

        // 读取 CSV 文件
        DataStream<Row> dataStream = ExponentialSmoothing.readCsvFile(env, filePath);

        // 设置平滑因子
        double smoothingFactor = 0.5;

        // 执行指数平滑计算
        DataStream<Row> resultStream = new ExponentialSmoothing().computeExponentialSmoothing(dataStream, smoothingFactor);

        // 打印每个窗口的指数平滑结果
        resultStream
                .map(row -> row.getField(0))  // 将 Row 转换为字符串，输出计算的指数平滑值
                .print();  // 打印输出

        // 执行 Flink 作业
        env.execute("Exponential Smoothing Test");
    }

    /*@Test
    public void testDataLessThanSmoothingFactor() throws Exception {
        // 创建一个少量数据的流
        DataStream<Row> smallDataStream = env.fromElements(
                Row.of(757900800000L, 112.0),
                Row.of(758160000000L, 118.0),
                Row.of(758246400000L, 132.0)
        );

        // 设置平滑因子
        double smoothingFactor = 0.5;

        // 执行指数平滑计算
        DataStream<Row> resultStream = new ExponentialSmoothing().computeExponentialSmoothing(smallDataStream, smoothingFactor);

        // 打印输出
        resultStream
                .map(row -> row.getField(0))  // 输出计算的指数平滑值
                .print();

        // 执行 Flink 作业
        env.execute("Data Less Than Smoothing Factor Test");
    }*/

    /*@Test
    public void testDataWithNulls() throws Exception {
        // 创建包含 null 值的数据流
        DataStream<Row> dataWithNullsStream = env.fromElements(
                Row.of(757900800000L, 112.0),
                Row.of(758160000000L, null),  // 这个值是 null
                Row.of(758246400000L, 132.0)
        );

        // 设置平滑因子
        double smoothingFactor = 0.5;

        // 执行指数平滑计算
        DataStream<Row> resultStream = new ExponentialSmoothing().computeExponentialSmoothing(dataWithNullsStream, smoothingFactor);

        // 打印输出
        resultStream
                .map(row -> row.getField(0))  // 输出计算的指数平滑值
                .print();

        // 执行 Flink 作业
        env.execute("Data With Nulls Test");
    }*/
}