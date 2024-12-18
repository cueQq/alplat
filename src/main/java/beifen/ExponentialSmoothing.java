//
// Source code for Exponential Smoothing
// Following Flink ML API specifications.
//

package beifen;

import org.apache.flink.ml.api.AlgoOperator;
import org.apache.flink.ml.common.param.HasFeaturesCol;
import org.apache.flink.ml.common.param.HasPredictionCol;
import org.apache.flink.ml.param.DoubleParam;
import org.apache.flink.ml.param.Param;

import org.apache.flink.ml.param.ParamValidators;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ExponentialSmoothing implements AlgoOperator<ExponentialSmoothing>, ExponentialSmoothingParams<ExponentialSmoothing> {

    private final Map<Param<?>, Object> paramMap = new HashMap<>();

    public ExponentialSmoothing() {
        ParamUtils.initializeMapWithDefaultValues(this.paramMap, this);
    }

    @Override
    public Table[] transform(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1, "Exponential Smoothing expects a single input table.");

        StreamTableEnvironment tEnv = (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        DataStream<Row> dataStream = tEnv.toDataStream(inputs[0]);

        Double smoothingFactor = this.getSmoothingFactor();
        String featuresCol = this.getFeaturesCol();
        String predictionCol = this.getPredictionCol();

        SingleOutputStreamOperator<Row> smoothedStream = dataStream.process(new ExponentialSmoothingFunction(featuresCol, predictionCol, smoothingFactor));

        Schema schema = Schema.newBuilder()
                .fromResolvedSchema(inputs[0].getResolvedSchema())
                .column(predictionCol, DataTypes.DOUBLE())
                .build();

        Table outputTable = tEnv.fromDataStream(smoothedStream, schema);
        return new Table[]{outputTable};
    }

    /**@Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }


    public static ExponentialSmoothing load(StreamTableEnvironment tEnv, String path) throws IOException {
        return (ExponentialSmoothing) ReadWriteUtils.loadStageParam(path);
    }**/

    @Override
    public void save(String path) throws IOException {
        // 保存仅参数信息
        ReadWriteUtils.saveMetadata(this, path);
    }

    public void save(StreamTableEnvironment tEnv, String path) throws IOException {
        // 如果需要保存模型额外信息，可以使用 tEnv
        save(path); // 调用基本保存方法
    }

   /** public static ExponentialSmoothing load(String path) throws IOException {
        return (ExponentialSmoothing) ReadWriteUtils.loadStageParam(path);
    }**/
   public static ExponentialSmoothing load(StreamTableEnvironment tEnv, String path) throws IOException {
       // 如果需要用到 tEnv，这里可以增加逻辑，否则 tEnv 参数可以忽略
       return (ExponentialSmoothing) ReadWriteUtils.loadStageParam(path);
   }


    @Override
    public Map<Param<?>, Object> getParamMap() {
        return this.paramMap;
    }

    private static class ExponentialSmoothingFunction extends org.apache.flink.streaming.api.functions.ProcessFunction<Row, Row> {

        private final String featuresCol;
        private final String predictionCol;
        private final Double smoothingFactor;
        private Double previousValue = null;

        public ExponentialSmoothingFunction(String featuresCol, String predictionCol, Double smoothingFactor) {
            this.featuresCol = featuresCol;
            this.predictionCol = predictionCol;
            this.smoothingFactor = smoothingFactor;
        }

        @Override
        public void processElement(Row value, Context ctx, Collector<Row> out) {
            Double currentValue = ((Number) value.getField(featuresCol)).doubleValue();
            if (previousValue == null) {
                previousValue = currentValue;
            } else {
                previousValue = smoothingFactor * currentValue + (1 - smoothingFactor) * previousValue;
            }
            Row outputRow = Row.join(value, Row.of(previousValue));
            out.collect(outputRow);
        }
    }
}

interface ExponentialSmoothingParams<T> extends HasFeaturesCol<T>, HasPredictionCol<T> {

    Param<Double> SMOOTHING_FACTOR = new DoubleParam(
            "smoothingFactor",
            "The smoothing factor for exponential smoothing (between 0 and 1).",
            0.5,
            ParamValidators.inRange(0.0, 1.0)
    );

    default Double getSmoothingFactor() {
        return this.get(SMOOTHING_FACTOR);
    }

    default T setSmoothingFactor(Double value) {
        return this.set(SMOOTHING_FACTOR, value);
    }


}
