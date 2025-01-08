package org.example;

import FeatureGeneration.TimeFeatureExtractor2;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.Arrays;
import java.util.List;

public class TimeFeatureExtractorTest2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 模拟输入数据
        List<Row> inputData = Arrays.asList(
                Row.of("2023-10-26"),
                Row.of("2024-01-15"),
                Row.of("2023-12-31"),
                Row.of("2024-02-29"),
                Row.of("2023-05-08"),
                Row.of((String) null),  // 测试 null 值
                Row.of("2023/10/26"), // 测试格式错误的日期
                Row.of("2023-1-1")   // 测试格式错误的日期
        );

        System.out.println("\n=============== 模拟数据时间特征提取 ===============");

        // 定义 RowTypeInfo
        String[] inputCols = new String[]{"date"};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(new org.apache.flink.api.common.typeinfo.TypeInformation[]{Types.STRING}, inputCols);


        // 定义输入数据 DataStream，指定类型信息
        DataStream<Row> dataStream = env.fromCollection(inputData, rowTypeInfo);


        // 定义输入表
        Schema inputSchema = Schema.newBuilder()
                .column("date", DataTypes.STRING())
                .build();
        Table inputTable = tEnv.fromDataStream(dataStream,inputSchema);


        // 创建并配置 TimeFeatureExtractor2 实例
        TimeFeatureExtractor2 timeFeatureExtractor = new TimeFeatureExtractor2()
                .setInputCol("date")
                .setOutputCols("year", "month", "day");


        // 执行算法
        Table resultTable = timeFeatureExtractor.transform(inputTable)[0];

        // 收集并打印结果
        try (CloseableIterator<Row> iterator = resultTable.execute().collect()) {
            iterator.forEachRemaining(row -> {
                System.out.println(row);
            });
        }

        env.execute("Time Feature Extractor Test");
    }
}