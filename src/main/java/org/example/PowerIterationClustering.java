package org.example;

import org.apache.calcite.interpreter.Row;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.api.DataTypes;



import org.apache.flink.ml.feature.minmaxscaler.MinMaxScaler;
import org.apache.flink.ml.feature.stringindexer.StringIndexer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.Random;

public class PowerIterationClustering {
//
//    public static void main(String[] args) throws Exception {
//        // 创建 Flink 环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//
//        // 读取 CSV 数据集
//        String filePath = "path/to/your/dataset.csv";
//        tableEnv.connect(new FileSystem().path(filePath))
//                .withFormat(new Csv().fieldDelimiter(',').deriveSchema())
//                .withSchema(new Schema()
//                        .field("categoricalColumn", DataTypes.STRING())
//                        .field("numericColumn", DataTypes.DOUBLE())
//                        .field("id", DataTypes.INT()))
//                .createTemporaryTable("InputTable");
//
//        Table inputTable = tableEnv.from("InputTable");
//
//        // 数据预处理 - 标签编码和归一化
//        StringIndexer stringIndexer = new StringIndexer()
//                .setInputCol("categoricalColumn")
//                .setOutputCol("indexedColumn");
//
//        Table[] encodedTable = stringIndexer.fit(inputTable).transform(inputTable);
//
//        MinMaxScaler minMaxScaler = new MinMaxScaler()
//                .setInputCol("numericColumn")
//                .setOutputCol("scaledColumn");
//
//        Table[] normalizedTable = minMaxScaler.fit(encodedTable).transform(encodedTable);
//
//        // 转换为 DataStream
//        DataStream<Tuple2<Integer, double[]>> matrixStream = tableEnv.toDataStream(normalizedTable)
//                .flatMap(new TableToMatrixFunction());
//
//        DataStream<double[]> vectorStream = env.fromElements(generateRandomVector(4));
//
//        // 进行幂迭代
//        for (int i = 0; i < 10; i++) { // 进行10次迭代
//            vectorStream = matrixStream
//                    .flatMap(new MultiplyMatrixVector())
//                    .keyBy(0)
//                    .sum(1)
//                    .map(new NormalizeVector());
//        }
//
//        // 输出最终结果
//        vectorStream.print();
//
//        // 执行 Flink 作业
//        env.execute("Power Iteration Algorithm with Real Dataset");
//    }
//
//    // 生成随机向量
//    private static double[] generateRandomVector(int size) {
//        double[] vector = new double[size];
//        Random random = new Random();
//        for (int i = 0; i < size; i++) {
//            vector[i] = random.nextDouble();
//        }
//        return vector;
//    }
//
//    // 表转换为矩阵行
//    public static class TableToMatrixFunction implements FlatMapFunction<Row, Tuple2<Integer, double[]>> {
//        @Override
//        public void flatMap(Row row, Collector<Tuple2<Integer, double[]>> out) {
//            double[] values = new double[row.getArity() - 1];
//            for (int i = 1; i < row.getArity(); i++) {
//                values[i - 1] = ((Number) row.getField(i)).doubleValue();
//            }
//            out.collect(new Tuple2<>((Integer) row.getField(0), values));
//        }
//    }
//
//    // 矩阵和向量相乘
//    public static class MultiplyMatrixVector implements FlatMapFunction<Tuple2<Integer, double[]>, Tuple2<Integer, Double>> {
//        @Override
//        public void flatMap(Tuple2<Integer, double[]> matrixRow, Collector<Tuple2<Integer, Double>> out) {
//            double[] row = matrixRow.f1;
//            double sum = 0;
//            for (double v : row)sum += v;
//            out.collect(new Tuple2<>(matrixRow.f0, sum));
//        }
//    }
//
//    // 归一化向量
//    public static class NormalizeVector extends RichMapFunction<Tuple2<Integer, Double>, double[]> {
//        @Override
//        public double[] map(Tuple2<Integer, Double> value) {
//            // 简单实现: 每个值除以向量的模
//            double magnitude = Math.sqrt(value.f1 * value.f1);
//            double[] normalized = new double[1];
//            normalized[0] = value.f1 / magnitude;
//            return normalized;
//        }
//    }

}