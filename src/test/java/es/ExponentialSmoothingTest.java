package es;

import beifen.ExponentialSmoothing;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.assertEquals;

public class ExponentialSmoothingTest {

    private StreamTableEnvironment tEnv;

    @Before
    public void setup() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
    }

    @Test
    public void testExponentialSmoothing() throws Exception {
        // 1. Create input data
        List<Row> inputData = Arrays.asList(
                Row.of(100.0),
                Row.of(110.0),
                Row.of(120.0),
                Row.of(130.0)
        );

        Table inputTable1 = tEnv.fromValues(Types.ROW(Types.DOUBLE), inputData);
        DataStream<Row> inputDataStream = tEnv.toDataStream(inputTable1, Row.class);
        Table inputTable =  tEnv.fromDataStream(inputDataStream);

        // 2. Create ExponentialSmoothing instance and set parameters
        ExponentialSmoothing es = new ExponentialSmoothing()
                .setFeaturesCol("f0")
                .setPredictionCol("smoothed_value")
                .setSmoothingFactor(0.3);

        // 3. Apply transform
        Table[] outputTable = es.transform(inputTable);


        // Convert Table to List of Rows
        List<Row> resultRows = (List<Row>) tEnv.toDataStream(outputTable[0], Row.class).executeAndCollect();

        //4. Assert results
        assertEquals(4, resultRows.size());
        assertEquals(100.0,resultRows.get(0).getField(1));
        assertEquals(103.0, Math.round(Double.parseDouble(resultRows.get(1).getField(1).toString())));
        assertEquals(108.1, Math.round(Double.parseDouble(resultRows.get(2).getField(1).toString())*10)/10.0);
        assertEquals(114.6, Math.round(Double.parseDouble(resultRows.get(3).getField(1).toString())*10)/10.0);


    }
    @Test
    public void testExponentialSmoothingWithCustomSchema() throws Exception {
        // 1. Create input data with custom schema
        List<Row> inputData = Arrays.asList(
                Row.of("2024-01-01", 100.0),
                Row.of("2024-01-02", 110.0),
                Row.of("2024-01-03", 120.0),
                Row.of("2024-01-04", 130.0)
        );
        Table inputTable1 = tEnv.fromValues(Types.ROW(Types.STRING, Types.DOUBLE), inputData).as("date","value");
        DataStream<Row> inputDataStream = tEnv.toDataStream(inputTable1, Row.class);
        Table inputTable = tEnv.fromDataStream(inputDataStream);

        // 2. Create ExponentialSmoothing instance and set parameters
        ExponentialSmoothing es = new ExponentialSmoothing()
                .setFeaturesCol("value")
                .setPredictionCol("smoothed_value")
                .setSmoothingFactor(0.3);

        // 3. Apply transform
        Table[] outputTable = es.transform(inputTable);

        // Convert Table to List of Rows
        List<Row> resultRows = (List<Row>) tEnv.toDataStream(outputTable[0], Row.class).executeAndCollect();

        //4. Assert results
        assertEquals(4, resultRows.size());
        assertEquals(100.0,resultRows.get(0).getField(2));
        assertEquals(103.0, Math.round(Double.parseDouble(resultRows.get(1).getField(2).toString())));
        assertEquals(108.1, Math.round(Double.parseDouble(resultRows.get(2).getField(2).toString())*10)/10.0);
        assertEquals(114.6, Math.round(Double.parseDouble(resultRows.get(3).getField(2).toString())*10)/10.0);


    }
}