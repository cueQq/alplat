/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package es;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.common.param.HasHandleInvalid;
import org.apache.flink.ml.feature.onehotencoder.OneHotEncoder;
import org.apache.flink.ml.feature.onehotencoder.OneHotEncoderModel;
import org.apache.flink.ml.feature.onehotencoder.OneHotEncoderModelData;
import org.apache.flink.ml.linalg.Vector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/** Tests OneHotEncoder and OneHotEncoderModel. */
public class OneHotEncoderTest extends AbstractTestBase {
    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;
    private Table trainTable;
    private Table predictTable;
    private Map<Double, Vector>[] expectedOutput;
    private OneHotEncoder estimator;

    @Before
    public void before() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);

        List<Row> trainData = Arrays.asList(Row.of(0.0), Row.of(1.0), Row.of(2.0), Row.of(0.0));

        trainTable = tEnv.fromDataStream(env.fromCollection(trainData)).as("input");

        List<Row> predictData = Arrays.asList(Row.of(0.0), Row.of(1.0), Row.of(2.0));

        predictTable = tEnv.fromDataStream(env.fromCollection(predictData)).as("input");

        expectedOutput =
                new HashMap[] {
                        new HashMap<Double, Vector>() {
                            {
                                put(0.0, Vectors.sparse(2, new int[] {0}, new double[] {1.0}));
                                put(1.0, Vectors.sparse(2, new int[] {1}, new double[] {1.0}));
                                put(2.0, Vectors.sparse(2, new int[0], new double[0]));
                            }
                        }
                };

        estimator = new OneHotEncoder().setInputCols("input").setOutputCols("output");
    }

    /**
     * Executes a given table and collect its results. Results are returned as a map array. Each
     * element in the array is a map corresponding to a input column whose key is the original value
     * in the input column, value is the one-hot encoding result of that value.
     *
     * @param table A table to be executed and to have its result collected
     * @param inputCols Name of the input columns
     * @param outputCols Name of the output columns containing one-hot encoding result
     * @return An array of map containing the collected results for each input column
     */
    private static Map<Double, Vector>[] executeAndCollect(
            Table table, String[] inputCols, String[] outputCols) {
        Map<Double, Vector>[] maps = new HashMap[inputCols.length];
        for (int i = 0; i < inputCols.length; i++) {
            maps[i] = new HashMap<>();
        }
        for (CloseableIterator<Row> it = table.execute().collect(); it.hasNext(); ) {
            Row row = it.next();
            for (int i = 0; i < inputCols.length; i++) {
                maps[i].put(
                        ((Number) row.getField(inputCols[i])).doubleValue(),
                        (Vector) row.getField(outputCols[i]));
            }
        }
        return maps;
    }

    @Test
    public void testParam() {
        OneHotEncoder estimator = new OneHotEncoder();

        assertTrue(estimator.getDropLast());

        estimator.setInputCols("test_input").setOutputCols("test_output").setDropLast(false);

        assertArrayEquals(new String[] {"test_input"}, estimator.getInputCols());
        assertArrayEquals(new String[] {"test_output"}, estimator.getOutputCols());
        assertFalse(estimator.getDropLast());

        OneHotEncoderModel model = new OneHotEncoderModel();

        assertTrue(model.getDropLast());

        model.setInputCols("test_input").setOutputCols("test_output").setDropLast(false);

        assertArrayEquals(new String[] {"test_input"}, model.getInputCols());
        assertArrayEquals(new String[] {"test_output"}, model.getOutputCols());
        assertFalse(model.getDropLast());
    }

    @Test
    public void testFitAndPredict() {
        OneHotEncoderModel model = estimator.fit(trainTable);
        Table outputTable = model.transform(predictTable)[0];
        Map<Double, Vector>[] actualOutput =
                executeAndCollect(outputTable, model.getInputCols(), model.getOutputCols());
        assertArrayEquals(expectedOutput, actualOutput);
    }

    @Test
    public void testInputTypeConversion() throws Exception {
        // 将数据类型转换为稀疏整数
        trainTable = tEnv.sqlQuery("SELECT CAST(input AS INT) AS input FROM " + trainTable);
        predictTable = tEnv.sqlQuery("SELECT CAST(input AS INT) AS input FROM " + predictTable);

// 获取列的类型并断言
        Class<?>[] trainColumnTypes = trainTable.getResolvedSchema()
                .getColumnDataTypes()
                .stream()
                .map(dataType -> dataType.getConversionClass())
                .toArray(Class<?>[]::new);

        Class<?>[] predictColumnTypes = predictTable.getResolvedSchema()
                .getColumnDataTypes()
                .stream()
                .map(dataType -> dataType.getConversionClass())
                .toArray(Class<?>[]::new);

        assertArrayEquals(new Class<?>[] {Integer.class}, trainColumnTypes);
        assertArrayEquals(new Class<?>[] {Integer.class}, predictColumnTypes);

        OneHotEncoderModel model = estimator.fit(trainTable);
        Table outputTable = model.transform(predictTable)[0];
        Map<Double, Vector>[] actualOutput =
                executeAndCollect(outputTable, model.getInputCols(), model.getOutputCols());
        assertArrayEquals(expectedOutput, actualOutput);
    }

    @Test
    public void testDropLast() {
        estimator.setDropLast(false);

        expectedOutput =
                new HashMap[] {
                        new HashMap<Double, Vector>() {
                            {
                                put(0.0, Vectors.sparse(3, new int[] {0}, new double[] {1.0}));
                                put(1.0, Vectors.sparse(3, new int[] {1}, new double[] {1.0}));
                                put(2.0, Vectors.sparse(3, new int[] {2}, new double[] {1.0}));
                            }
                        }
                };

        OneHotEncoderModel model = estimator.fit(trainTable);
        Table outputTable = model.transform(predictTable)[0];
        Map<Double, Vector>[] actualOutput =
                executeAndCollect(outputTable, model.getInputCols(), model.getOutputCols());
        assertArrayEquals(expectedOutput, actualOutput);
    }

    @Test
    public void testInputDataType() {
        List<Row> trainData = Arrays.asList(Row.of(0), Row.of(1), Row.of(2), Row.of(0));

        trainTable = tEnv.fromDataStream(env.fromCollection(trainData)).as("input");

        List<Row> predictData = Arrays.asList(Row.of(0), Row.of(1), Row.of(2));
        predictTable = tEnv.fromDataStream(env.fromCollection(predictData)).as("input");

        expectedOutput =
                new HashMap[] {
                        new HashMap<Double, Vector>() {
                            {
                                put(0.0, Vectors.sparse(2, new int[] {0}, new double[] {1.0}));
                                put(1.0, Vectors.sparse(2, new int[] {1}, new double[] {1.0}));
                                put(2.0, Vectors.sparse(2, new int[0], new double[0]));
                            }
                        }
                };

        OneHotEncoderModel model = estimator.fit(trainTable);
        Table outputTable = model.transform(predictTable)[0];
        Map<Double, Vector>[] actualOutput =
                executeAndCollect(outputTable, model.getInputCols(), model.getOutputCols());
        assertArrayEquals(expectedOutput, actualOutput);
    }

    @Test
    public void testNotSupportedHandleInvalidOptions() {
        estimator.setHandleInvalid(HasHandleInvalid.SKIP_INVALID);
        try {
            estimator.fit(trainTable);
            Assert.fail("Expected IllegalArgumentException");
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, ((Throwable) e).getClass());
        }
    }

    @Test
    public void testNonIndexedTrainData() {
        List<Row> trainData = Arrays.asList(Row.of(0.5), Row.of(1.0), Row.of(2.0), Row.of(0.0));

        trainTable = tEnv.fromDataStream(env.fromCollection(trainData)).as("input");
        OneHotEncoderModel model = estimator.fit(trainTable);
        Table outputTable = model.transform(predictTable)[0];
        try {
            outputTable.execute().collect().next();
            Assert.fail("Expected IllegalArgumentException");
        } catch (Exception e) {
            Throwable exception = ExceptionUtils.getRootCause(e);
            assertEquals(IllegalArgumentException.class, exception.getClass());
            assertEquals("Value 0.5 cannot be parsed as indexed integer.", exception.getMessage());
        }
    }

    @Test
    public void testNonIndexedPredictData() {
        List<Row> predictData = Arrays.asList(Row.of(0.5), Row.of(1.0), Row.of(2.0), Row.of(0.0));

        predictTable = tEnv.fromDataStream(env.fromCollection(predictData)).as("input");
        OneHotEncoderModel model = estimator.fit(trainTable);
        Table outputTable = model.transform(predictTable)[0];
        try {
            outputTable.execute().collect().next();
            Assert.fail("Expected IllegalArgumentException");
        } catch (Exception e) {
            Throwable exception = e;
            while (exception.getCause() != null) {
                exception = exception.getCause();
            }
            assertEquals(IllegalArgumentException.class, exception.getClass());
            assertEquals("Value 0.5 cannot be parsed as indexed integer.", exception.getMessage());
        }
    }

    @Test
    public void testSaveLoad() throws Exception {
        // 保存 estimator 到指定路径
        String estimatorPath = tempFolder.newFolder().getAbsolutePath();
        estimator.save(estimatorPath);

// 从保存路径加载 estimator
        OneHotEncoder reloadedEstimator = OneHotEncoder.load(tEnv, estimatorPath);

// 使用加载后的 estimator 进行模型训练
        OneHotEncoderModel model = reloadedEstimator.fit(trainTable);

// 保存模型到指定路径
        String modelPath = tempFolder.newFolder().getAbsolutePath();
        model.save(modelPath);

// 从保存路径加载模型
        OneHotEncoderModel reloadedModel = OneHotEncoderModel.load(tEnv, modelPath);

        Table outputTable = model.transform(predictTable)[0];
        Map<Double, Vector>[] actualOutput =
                executeAndCollect(outputTable, model.getInputCols(), model.getOutputCols());
        assertArrayEquals(expectedOutput, actualOutput);
    }

    @Test
    public void testGetModelData() throws Exception {
        OneHotEncoderModel model = estimator.fit(trainTable);
        Tuple2<Integer, Integer> expected = new Tuple2<>(0, 2);
        Tuple2<Integer, Integer> actual =
                OneHotEncoderModelData.getModelDataStream(model.getModelData()[0])
                        .executeAndCollect()
                        .next();
        assertEquals(expected, actual);
    }

    @Test
    public void testSetModelData() {
        OneHotEncoderModel modelA = estimator.fit(trainTable);

        Table modelData = modelA.getModelData()[0];
        OneHotEncoderModel modelB = new OneHotEncoderModel().setModelData(modelData);
        ParamUtils.updateExistingParams(modelB, modelA.getParamMap());

        Table outputTable = modelB.transform(predictTable)[0];
        Map<Double, Vector>[] actualOutput =
                executeAndCollect(outputTable, modelB.getInputCols(), modelB.getOutputCols());
        assertArrayEquals(expectedOutput, actualOutput);
    }
}
