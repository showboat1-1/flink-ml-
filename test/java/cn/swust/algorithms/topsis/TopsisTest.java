package cn.swust.algorithms.topsis;


import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.ml.api.Stage;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.BiFunctionWithException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TopsisTest extends AbstractTestBase {
    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();
    private StreamTableEnvironment tEnv;
    private StreamExecutionEnvironment env;
    private Table inputDataTable;

    private final List<DenseVector> INPUT_DATA =
            Arrays.asList(
                    Vectors.dense(9.0,10.0,175.0,120.0),
                    Vectors.dense(8.0,7.0,164.0,80.0),
                    Vectors.dense(6.0,3.0,157.0,90.0));
    private final List<Row> exceptOutputData =
            Arrays.asList(
                    Row.of(Vectors.dense(9.0,10.0,175.0,120.0),Vectors.dense(0.2033)),
                    Row.of(Vectors.dense(8.0,7.0,164.0,80.0),Vectors.dense(0.5905)),
                    Row.of(Vectors.dense(6.0,3.0,157.0,90.0),Vectors.dense(0.6247))
            );

    /**
     * Gets a {@link StreamExecutionEnvironment} with the most common configurations of the Flink ML
     * program.
     */
    public static StreamExecutionEnvironment getExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        env.getConfig().disableGenericTypes();
        env.setParallelism(4);//Test中限制了并发度不能大于4
        env.enableCheckpointing(100);
        env.setRestartStrategy(RestartStrategies.noRestart());
        return env;
    }

    public static <T extends Stage<T>> T saveAndReload(
            StreamTableEnvironment tEnv,
            T stage,
            String path,
            BiFunctionWithException<StreamTableEnvironment, String, T, IOException> loadFunc)
            throws Exception {
        StreamExecutionEnvironment env = TableUtils.getExecutionEnvironment(tEnv);

        stage.save(path);
        try {
            env.execute();
        } catch (RuntimeException e) {
            if (!e.getMessage()
                    .equals("No operators defined in streaming topology. Cannot execute.")) {
                throw e;
            }
        }

        return loadFunc.apply(tEnv, path);
    }

    @Before
    public void before() {
        env = getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        //构建数据源
        DataStream<DenseVector> dataStream = env.fromCollection(INPUT_DATA);
        inputDataTable = tEnv.fromDataStream(dataStream).as("features");
    }

    @Test
    public void testParam() {
        Topsis topsis = new Topsis();
        topsis.setCriteriaTypes(1, 2, 3, 4);
        topsis.setWeights(0.4,0.3,0.2,0.1);
        topsis.setBestValue(165.0);
        topsis.setIntervalValue(90.0,100.0);

        assertArrayEquals(new Integer[]{1, 2, 3, 4}, topsis.getCriteriaTypes());
        assertArrayEquals(new Double[]{0.4,0.3,0.2,0.1}, topsis.getWeights());
        assertEquals(165.0, topsis.getBestValue(), 0.00001);
        assertArrayEquals(new Double[]{90.0,100.0}, topsis.getIntervalValue());
        assertEquals("features",topsis.getFeaturesCol());
        assertEquals("prediction",topsis.getPredictionCol());
    }

    @Test
    public void testOutputSchema() {
        Table input = inputDataTable.as("test_feature");
        Topsis topsis = new Topsis();
        topsis.setCriteriaTypes(1, 2, 3, 4);
        topsis.setWeights(0.4,0.3,0.2,0.1);
        topsis.setBestValue(165.0);
        topsis.setIntervalValue(90.0,100.0);
        topsis.setFeaturesCol("test_feature").setPredictionCol("test_prediction");

        Table output = topsis.transform(input)[0];

        assertEquals(
                Arrays.asList("test_feature","test_prediction"),
                output.getResolvedSchema().getColumnNames());
    }

    @Test
    public void testSaveLoadTransform() throws Exception {
        Topsis topsis = new Topsis();
        topsis.setCriteriaTypes(1, 2, 3, 4);
        topsis.setWeights(0.4,0.3,0.2,0.1);
        topsis.setBestValue(165.0);
        topsis.setIntervalValue(90.0,100.0);

        topsis =
                saveAndReload(
                        tEnv,
                        topsis,
                        tempFolder.newFolder().getAbsolutePath(),
                        Topsis::load);

        Table[] outputs = topsis.transform(inputDataTable);

        DataStream<Row> rowDataStream = tEnv.toDataStream(outputs[0]);
        List<Row> results = IteratorUtils.toList(rowDataStream.executeAndCollect());
        verifyOutputResult(results,exceptOutputData);
    }

    private void verifyOutputResult(List<Row> results, List<Row> exceptOutputData) {
        //排序
        results.sort((o1, o2) -> Objects.hashCode(o1.getField(0)) < Objects.hashCode(o2.getField(0)) ? 1 : -1);
        exceptOutputData.sort((o1, o2) -> Objects.hashCode(o1.getField(0)) < Objects.hashCode(o2.getField(0)) ? 1 : -1);
        //比较两者大小
        assertEquals(results.size(), exceptOutputData.size());
        //比较每一个元素
        for (int i = 0; i < results.size(); i++) {
            DenseVector dv1 = (DenseVector) results.get(i).getField(1);
            DenseVector dv2 = (DenseVector) exceptOutputData.get(i).getField(1);
            assertArrayEquals(dv1.toArray(),dv2.toArray(),0.0001);
        }
    }
}
