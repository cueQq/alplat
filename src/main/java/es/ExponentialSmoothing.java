package es;



import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.ml.api.AlgoOperator;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ExponentialSmoothing implements AlgoOperator<ExponentialSmoothing>, ExponentialSmoothingParams<ExponentialSmoothing> {

    private final Map<Param<?>, Object> paramMap = new HashMap<>();

    public ExponentialSmoothing() {
        // 初始化参数默认值
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public Table[] transform(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);

        StreamTableEnvironment tEnv = (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        DataStream<Row> dataStream = tEnv.toDataStream(inputs[0]);

        Double smoothingFactor = getSmoothingFactor();

        DataStream<Row> resultStream = computeExponentialSmoothing(dataStream, smoothingFactor);

        // 创建结果表的 Schema
        Schema schema = Schema.newBuilder()
                .column("f0", DataTypes.DOUBLE())  // 显式使用默认的列名 f0
                .build();

        // 将结果转换为 Table
        Table resultTable = tEnv.fromDataStream(resultStream, schema);

        return new Table[]{resultTable};  // 返回包含计算结果的 Table
    }

    public DataStream<Row> computeExponentialSmoothing(DataStream<Row> inputStream, Double smoothingFactor) {
        return inputStream
                // 为每个元素分配时间戳和水印
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(Row element) {
                        Object timestampObj = element.getField(0);
                        if (timestampObj instanceof Long) {
                            return (Long) timestampObj;
                        } else {
                            throw new RuntimeException("Invalid timestamp field in row");
                        }
                    }
                })
                .keyBy(row -> 1)  // 使用常量键来模拟全局窗口
                .timeWindow(Time.seconds(1))  // 使用1秒的窗口
                .apply(new ExponentialSmoothingFunction(smoothingFactor));  // 使用 ExponentialSmoothingFunction 计算指数平滑
    }


    private static class ExponentialSmoothingFunction implements WindowFunction<Row, Row, Integer, TimeWindow> {
        private final Double smoothingFactor;
        private Double lastSmoothedValue = null;

        public ExponentialSmoothingFunction(Double smoothingFactor) {
            this.smoothingFactor = smoothingFactor;
        }

        @Override
        public void apply(Integer key, TimeWindow window, Iterable<Row> input, Collector<Row> out) {
            for (Row row : input) {
                Double value = (Double) row.getField(1);  // 假设第二个字段是数值
                if (value == null) {
                    value = 0.0;  // 使用默认值处理 null
                }

                if (lastSmoothedValue == null) {
                    lastSmoothedValue = value;  // 初始化第一个平滑值
                } else {
                    lastSmoothedValue = smoothingFactor * value + (1 - smoothingFactor) * lastSmoothedValue;
                }

                // 只输出指数平滑的结果
                out.collect(Row.of(lastSmoothedValue));
            }
        }
    }


    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    // CSV 文件的读取
    public static DataStream<Row> readCsvFile(StreamExecutionEnvironment env, String path) {
        return env.readTextFile(path)
                .map(new MapFunction<String, Row>() {
                    @Override
                    public Row map(String line) throws Exception {
                        String[] fields = line.split(",");
                        long timestamp = 0L;
                        try {
                            timestamp = Long.parseLong(fields[0]);  // 假设第一列是时间戳
                        } catch (NumberFormatException e) {
                            //System.err.println("Invalid timestamp format: " + fields[0]);
                        }

                        double value = 0.0;
                        if (fields.length > 1 && !fields[1].isEmpty()) {
                            try {
                                value = Double.parseDouble(fields[1]);
                            } catch (NumberFormatException e) {
                                //System.err.println("Invalid number format in data: " + fields[1]);
                            }
                        }

                        return Row.of(timestamp, value);
                    }
                });
    }









    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }

    public static ExponentialSmoothing load(StreamTableEnvironment tEnv, String path) throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }

    // 获取平滑因子
    public Double getSmoothingFactor() {
        return (Double) paramMap.getOrDefault(ExponentialSmoothingParams.SMOOTHING_FACTOR, 0.5);  // 默认0.5
    }
}