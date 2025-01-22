package org.example;

import FeatureGeneration.Category.onehotencoder.OneHotEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.feature.onehotencoder.OneHotEncoderModel;
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

public class OneHotEncoderTest{

    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 模拟输入数据
        List<Row> inputData = Arrays.asList(
                Row.of(0),
                Row.of(1),
                Row.of(2),
                Row.of((Integer) null)
        );

        System.out.println("\n=============== 模拟数据一键编码 ===============");

        // 定义 RowTypeInfo
        String[] inputCols = new String[]{"category"}; // 指定字段名为 "category"
        RowTypeInfo rowTypeInfo = new RowTypeInfo(new org.apache.flink.api.common.typeinfo.TypeInformation[]{Types.INT}, inputCols);

        // 定义输入数据 DataStream, 指定类型信息
        DataStream<Row> dataStream = env.fromCollection(inputData, rowTypeInfo);

        // 定义输入表
        Schema inputSchema = Schema.newBuilder()
                .column("category", DataTypes.INT())  // 使用 "category"
                .build();
        Table inputTable = tEnv.fromDataStream(dataStream, inputSchema); // 使用 fromDataStream

        // 创建并配置 OneHotEncoder 实例
        OneHotEncoder encoder = new OneHotEncoder()
                .setInputCols(new String[]{"category"})
                .setOutputCols(new String[]{"encoded_category"});

        // Fit the estimator and get the model
        OneHotEncoderModel model = encoder.fit(inputTable);
        // Apply model
        Table outputTable = model.transform(inputTable)[0];

        // 收集并打印结果
        try (CloseableIterator<Row> iterator = outputTable.execute().collect()) {
            iterator.forEachRemaining(row -> {
                System.out.println(row);
            });
        }

        env.execute("OneHotEncoder Test");
    }
}