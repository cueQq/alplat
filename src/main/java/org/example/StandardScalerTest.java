package org.example;

import FeatureGeneration.NumericalValue.StandardScaler.StandardScaler;
import FeatureGeneration.NumericalValue.StandardScaler.StandardScalerModel;
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

public class StandardScalerTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 模拟输入数据
        List<Row> inputData = Arrays.asList(
                Row.of(10.0),
                Row.of(20.0),
                Row.of(30.0),
                Row.of((Double) null)
        );

        System.out.println("\n=============== 模拟数据标准化 ===============");

        // 定义 RowTypeInfo
        String[] inputCols = new String[]{"value"}; // 指定字段名为 "value"
        RowTypeInfo rowTypeInfo = new RowTypeInfo(new org.apache.flink.api.common.typeinfo.TypeInformation[]{Types.DOUBLE}, inputCols);

        // 定义输入数据 DataStream, 指定类型信息
        DataStream<Row> dataStream = env.fromCollection(inputData,rowTypeInfo);


        // 定义输入表
        Schema inputSchema = Schema.newBuilder()
                .column("value", DataTypes.DOUBLE())  // 使用 "value"
                .build();
        Table inputTable = tEnv.fromDataStream(dataStream, inputSchema); // 使用 fromDataStream


        // 创建并配置 StandardScaler 实例
        StandardScaler scaler = new StandardScaler()
                .setInputCol("value")
                .setOutputCol("scaled_value");

        //Fit the estimator and get the model
        StandardScalerModel model = scaler.fit(inputTable);
        //apply model
        Table outputTable = model.transform(inputTable)[0];


        // 收集并打印结果
        try (CloseableIterator<Row> iterator = outputTable.execute().collect()) {
            iterator.forEachRemaining(row -> {
                System.out.println(row);
            });
        }

        env.execute("Standard Scaler Test");
    }
}